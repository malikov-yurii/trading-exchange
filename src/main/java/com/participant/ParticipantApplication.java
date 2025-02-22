package com.participant;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A minimal Netty-based WebSocket client that:
 *  - Connects to ws://localhost:8080/ws by default
 *  - Sends N (default 2) buy orders and N sell orders
 *  - Logs requests and responses (including server's ExchangeResponse)
 *  - Waits for a shutdown signal
 *  - Catches and logs exceptions in parseExchangeResponse or channelRead0
 */
public class ParticipantApplication {
    private static final Logger log = LoggerFactory.getLogger(ParticipantApplication.class);
    private static final String DEFAULT_URI = "ws://localhost:8080/ws";
    private final String serverUri;
    private final int numOrders;
    private final AtomicLong orderSeqNum = new AtomicLong(1);

    private EventLoopGroup group;
    private Channel channel;

    public ParticipantApplication(String serverUri, int numOrders) {
        this.serverUri = serverUri;
        this.numOrders = numOrders;
    }

    public static void main(String[] args) throws Exception {
        String uri = (args.length > 0) ? args[0] : DEFAULT_URI;
        int orders = (args.length > 1) ? Integer.parseInt(args[1]) : 2;

        ParticipantApplication app = new ParticipantApplication(uri, orders);
        app.start();

        // Wait for a shutdown signal
        new ShutdownSignalBarrier().await();
        app.shutdown();
        log.info("ParticipantApplication terminated.");
    }

    public void start() throws Exception {
        log.info("Starting ParticipantApplication to {} with {} buy & {} sell orders...",
                serverUri, numOrders, numOrders);

        group = new NioEventLoopGroup(1);
        final URI uri = new URI(serverUri);
        final String scheme = (uri.getScheme() == null) ? "ws" : uri.getScheme();
        final String host = (uri.getHost() == null) ? "127.0.0.1" : uri.getHost();
        final int port = getPort(uri, scheme);

        final boolean ssl = "wss".equalsIgnoreCase(scheme);
        final SslContext sslCtx = ssl ? buildSslContext() : null;

        WebSocketClientHandler handler = new WebSocketClientHandler(uri);

        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        if (sslCtx != null) {
                            pipeline.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                        }
                        pipeline.addLast(new HttpClientCodec());
                        pipeline.addLast(new HttpObjectAggregator(8192));
                        pipeline.addLast(handler);
                    }
                });

        channel = b.connect(host, port).sync().channel();
        handler.handshakeFuture().sync(); // Wait for handshake

        log.info("Connected to server, handshake complete. Now sending orders...");

        // Send N buy orders, then N sell orders
        for (int i = 0; i < numOrders; i++) {
            sendOrder(2, 1, 100 + i, 100); // side 1 = SELL
            sendOrder(1, 0, 100 + i, 10); // side 0 = BUY
        }
    }

    /**
     * Build and send an order message in the same binary format the server expects:
     *  Layout:
     *   - 8 bytes: seq
     *   - 1 byte: side (0=BUY, 1=SELL)
     *   - 8 bytes: clientId
     *   - 8 bytes: tickerId
     *   - 8 bytes: orderId
     *   - 8 bytes: price
     *   - 8 bytes: qty
     */
    private void sendOrder(long clientId, int side, long price, long qty) {
        if (channel == null || !channel.isActive()) {
            log.error("Channel is not active, cannot send order!");
            return;
        }
        long seq = orderSeqNum.getAndIncrement();

        // We'll assume tickerId=1, orderId=seq
        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128); // Ensure enough capacity
        int offset = 0;

        buffer.putLong(offset, seq);
        offset += Long.BYTES;

        buffer.putByte(offset, (byte) side);
        offset += Byte.BYTES;

        buffer.putLong(offset, clientId);
        offset += Long.BYTES;

        buffer.putLong(offset, 1); // tickerId
        offset += Long.BYTES;

        buffer.putLong(offset, seq); // orderId
        offset += Long.BYTES;

        buffer.putLong(offset, price);
        offset += Long.BYTES;

        buffer.putLong(offset, qty);
        offset += Long.BYTES;

        byte[] data = new byte[offset];
        buffer.getBytes(0, data);

        ByteBuf nettyBuf = channel.alloc().buffer(offset);
        nettyBuf.writeBytes(data);

        BinaryWebSocketFrame frame = new BinaryWebSocketFrame(nettyBuf);

        log.info("Sending order: seq={}, clientId={}, side={}, {}@{}",
                seq, clientId, side == 0 ? "BUY" : "SELL", qty, price);

        channel.writeAndFlush(frame);
    }

    private int getPort(URI uri, String scheme) {
        if (uri.getPort() != -1) {
            return uri.getPort();
        }
        return ("wss".equalsIgnoreCase(scheme)) ? 443 : 80;
    }

    private SslContext buildSslContext() throws SSLException {
        return SslContextBuilder.forClient().build();
    }

    public void shutdown() {
        log.info("Shutting down ParticipantApplication...");
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    /**
     * A custom WebSocket client handler that performs the handshake, logs inbound frames, and
     * catches exceptions that occur during parseExchangeResponse or the decode pipeline.
     */
    private static class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
        private final URI uri;
        private ChannelPromise handshakeFuture;
        private WebSocketClientHandshaker handshaker;

        WebSocketClientHandler(URI uri) {
            this.uri = uri;
        }

        public ChannelFuture handshakeFuture() {
            return handshakeFuture;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());
            handshaker.handshake(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("WebSocket Client disconnected!");
        }

        /**
         * Top-level read method: catch exceptions from parse or other pipeline issues
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            try {
                handleMessage(ctx, msg);
            } catch (Exception e) {
                log.error("Error in channelRead0: {}", e.getMessage(), e);
                // Optionally close the channel to prevent netty from logging a pipeline error
                ctx.close();
            }
        }

        private void handleMessage(ChannelHandlerContext ctx, Object msg) {
            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                log.info("WebSocket Client connected! Handshake complete.");
                handshakeFuture.setSuccess();
                return;
            }

            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException("Unexpected FullHttpResponse: " + response.status());
            }

            if (msg instanceof TextWebSocketFrame) {
                TextWebSocketFrame textFrame = (TextWebSocketFrame) msg;
                log.info("Received Text frame: {}", textFrame.text());
            } else if (msg instanceof BinaryWebSocketFrame) {
                BinaryWebSocketFrame binFrame = (BinaryWebSocketFrame) msg;
                ByteBuf content = binFrame.content();
                // Force little-endian order to match server's output
                ByteBuf leBuf = content.order(ByteOrder.LITTLE_ENDIAN);
                ExchangeResponse exchangeResponse = parseExchangeResponse(leBuf);
                log.info("Received {}", exchangeResponse);
            } else if (msg instanceof PongWebSocketFrame) {
                log.info("Received Pong frame");
            } else if (msg instanceof CloseWebSocketFrame) {
                log.info("Received Close frame");
                ch.close();
            }
        }

        /**
         * ExchangeResponse format from server (little-endian):
         *  - 8 bytes: seqNum
         *  - 1 byte: type ordinal
         *  - 1 byte: side ordinal
         *  - 8 bytes: clientId
         *  - 8 bytes: tickerId
         *  - 8 bytes: clientOrderId
         *  - 8 bytes: marketOrderId
         *  - 8 bytes: price
         *  - 8 bytes: execQty
         *  - 8 bytes: leavesQty
         */
        private ExchangeResponse parseExchangeResponse(ByteBuf buf) {
            try {
                int offset = buf.readerIndex();

                long seqNum = buf.getLong(offset);
                offset += Long.BYTES;
                byte typeOrd = buf.getByte(offset++);
                ClientResponseType type = fromTypeOrd(typeOrd);
                byte sideOrd = buf.getByte(offset++);
                Side side = fromSideOrd(sideOrd);

                long clientId = buf.getLong(offset);
                offset += Long.BYTES;
                long tickerId = buf.getLong(offset);
                offset += Long.BYTES;
                long clientOrderId = buf.getLong(offset);
                offset += Long.BYTES;
                long marketOrderId = buf.getLong(offset);
                offset += Long.BYTES;
                long price = buf.getLong(offset);
                offset += Long.BYTES;
                long execQty = buf.getLong(offset);
                offset += Long.BYTES;
                long leavesQty = buf.getLong(offset);
                offset += Long.BYTES;

                ExchangeResponse resp = new ExchangeResponse();
                resp.setSeqNum(seqNum);
                resp.setType(type);
                resp.setSide(side);
                resp.setClientId(clientId);
                resp.setTickerId(tickerId);
                resp.setClientOrderId(clientOrderId);
                resp.setMarketOrderId(marketOrderId);
                resp.setPrice(price);
                resp.setExecQty(execQty);
                resp.setLeavesQty(leavesQty);

                return resp;
            } catch (Exception e) {
                log.error("Error parsing ExchangeResponse", e);
                // Optionally rethrow or return a partial object
                throw e;
            }
        }

        private ClientResponseType fromTypeOrd(byte ord) {
            ClientResponseType[] values = ClientResponseType.values();
            if (ord < 0 || ord >= values.length) {
                return ClientResponseType.INVALID;
            }
            return values[ord];
        }

        private Side fromSideOrd(byte ord) {
            Side[] sides = Side.values();
            if (ord < 0 || ord >= sides.length) {
                return Side.INVALID;
            }
            return sides[ord];
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("exceptionCaught in pipeline: {}", cause.getMessage(), cause);
            // Typically close on pipeline error
            ctx.close();
        }
    }
}