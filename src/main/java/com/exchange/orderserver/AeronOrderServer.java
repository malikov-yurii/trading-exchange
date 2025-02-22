package com.exchange.orderserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AeronOrderServer {

    private final LFQueue<ClientRequest> requestQueue;
    private final LFQueue<ClientResponse> responseQueue;

    private final AtomicLong reqSeqNum = new AtomicLong(1);
    private final AtomicLong respSeqNum = new AtomicLong(1);

    // clientId -> Netty Channel
    private final Map<Long, Channel> clients = new ConcurrentHashMap<>();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public AeronOrderServer(LFQueue<ClientRequest> requestQueue,
                            LFQueue<ClientResponse> responseQueue) {
        this.requestQueue = requestQueue;
        this.responseQueue = responseQueue;
    }

    public void start(int port) throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("boss"));
        workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("worker"));

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)      // For server channel
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT) // For child channels
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new HttpServerCodec());
                            pipeline.addLast(new HttpObjectAggregator(65536));
                            pipeline.addLast(new ChunkedWriteHandler());
                            // WebSocket on path "/ws"
                            pipeline.addLast(new WebSocketServerProtocolHandler("/ws", null, true));
                            pipeline.addLast(new WebSocketFrameHandler());
                        }
                    });

            serverChannel = b.bind(port).sync().channel();
            System.out.println("[OrderServer] Netty WebSocket server started on port " + port);

            responseQueue.subscribe(this::processResponse);

        } catch (Exception e) {
            System.err.println("Failed to start Netty server: " + e.getMessage());
            // Shutdown if error
            shutdown();
            throw e;
        }
    }

    private void processResponse(ClientResponse omResp) {
        if (omResp != null) {

            long seq = respSeqNum.getAndIncrement();
            omResp.setSeqNum(seq);

            long clientId = omResp.getClientId();
            Channel ch = clients.get(clientId);
            if (ch != null && ch.isActive()) {
                ByteBuf buffer = ch.alloc().directBuffer(64);
                buffer.writeLong(omResp.getSeqNum());
                buffer.writeByte(omResp.getType().ordinal());
                buffer.writeByte(omResp.getSide().ordinal());
                buffer.writeLong(omResp.getClientId());
                buffer.writeLong(omResp.getTickerId());
                buffer.writeLong(omResp.getClientOrderId());
                buffer.writeLong(omResp.getMarketOrderId());
                buffer.writeLong(omResp.getPrice());
                buffer.writeLong(omResp.getExecQty());
                buffer.writeLong(omResp.getLeavesQty());

                BinaryWebSocketFrame frame = new BinaryWebSocketFrame(buffer);
                ch.writeAndFlush(frame);

                System.out.println("[OrderServer] Sent response to clientId=" + clientId + ", seq=" + seq);
            }
        }
    }

    /**
     * Handler for incoming WebSocket frames.
     * We only care about BinaryWebSocketFrame.
     */
    private class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
            if (frame instanceof CloseWebSocketFrame) {
                ctx.channel().close();
            } else if (frame instanceof PingWebSocketFrame) {
                ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
            } else if (frame instanceof BinaryWebSocketFrame) {
                // parse the BinaryWebSocketFrame
                ByteBuf content = frame.content();
                ByteBuffer nioData = content.nioBuffer();
                ClientRequest omReq = deserializeClientRequest(nioData);

                long seq = reqSeqNum.getAndIncrement();
                omReq.setSeqNum(seq);

                long clientId = omReq.getClientId();
                clients.putIfAbsent(clientId, ctx.channel());

                // Put in the Disruptor queue
                requestQueue.offer(omReq);

                System.out.println("[OrderServer] Received request from clientId=" + clientId + ", seq=" + seq);
            } else {
                // For text frames or other frames, just discard or handle accordingly
                frame.release();
            }
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            // On close, remove from clients map
            Channel ch = ctx.channel();
            long cidToRemove = -1L;
            for (Map.Entry<Long, Channel> entry : clients.entrySet()) {
                if (entry.getValue() == ch) {
                    cidToRemove = entry.getKey();
                    break;
                }
            }
            if (cidToRemove >= 0) {
                clients.remove(cidToRemove);
                System.out.println("[OrderServer] Client disconnected, clientId=" + cidToRemove);
            }
            super.handlerRemoved(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    /**
     * Helper to (de)serialize data. Must match the layout used by your matching engine.
     */
    private ClientRequest deserializeClientRequest(ByteBuffer data) {

        ClientRequest req = new ClientRequest();
        long seq = data.getLong();
        req.setSeqNum(seq);

        byte typeOrd = data.get();
        byte sideOrd = data.get();
        req.setType(ClientRequestType.values()[typeOrd]);
        req.setSide(Side.values()[sideOrd]);

        req.setClientId(data.getLong());
        req.setTickerId(data.getLong());
        req.setOrderId(data.getLong());
        req.setPrice(data.getLong());
        req.setQty(data.getLong());
        return req;
    }

    /**
     * Shut down Netty
     */
    public void shutdown() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}