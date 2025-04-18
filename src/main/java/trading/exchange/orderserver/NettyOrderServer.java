package trading.exchange.orderserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderMessage;
import trading.api.OrderMessageSerDe;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.LFQueue;
import trading.common.Utils;
import trading.exchange.AppState;
import trading.exchange.LeadershipManager;
import trading.exchange.ReplayReplicationLogConsumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NettyOrderServer implements OrderServer{
    private static final Logger log = LoggerFactory.getLogger(NettyOrderServer.class);

    private final LFQueue<OrderRequest> clientRequests;
    private final LeadershipManager leadershipManager;
    private final AppState appState;

    private ReplicationConsumer replicationConsumer;
    private RequestSequencer requestSequencer;

    private final AtomicLong respSeqNum = new AtomicLong(1);

    private final Map<Long, Channel> channelsByClientId = new ConcurrentHashMap<>();
    private final Map<ChannelId, Long> clientIdByChannelId = new ConcurrentHashMap<>();

    private final String bindAddress;
    private final int listenPort;

    private Thread requestSequencerThread;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    private ShutdownSignalBarrier shutdownBarrier;
    private long seqNum;

    public NettyOrderServer(LFQueue<OrderRequest> clientRequests,
                            LFQueue<OrderMessage> clientResponses,
                            LeadershipManager leadershipManager,
                            AppState appState) {
        this.clientRequests = clientRequests;
        this.leadershipManager = leadershipManager;

        this.bindAddress = Utils.env("WS_IP", "0.0.0.0");
        this.listenPort = Integer.parseInt(Utils.env("WS_PORT", "8080"));

        clientResponses.subscribe(this::processResponse);
        this.appState = appState;
    }

    @Override
    public synchronized void start() {
        leadershipManager.onLeadershipAcquired(() -> {
            boolean wasRunning = stopReplicationConsumer();
            if (!wasRunning) {
                // Starting as leader right away, so we need to replay old client request to recover state
                appState.setRecovering();
                ReplayReplicationLogConsumer replayConsumer = new ReplayReplicationLogConsumer(clientRequests);
                replayConsumer.run();
                this.seqNum = replayConsumer.getLastSeqNum();
                appState.setRecovered();
            }
            startRequestSequencer();
            startNettyServer();
        });

        leadershipManager.onLeadershipLost(() -> {
            stopNettyServer();
            stopRequestSequencer();
            startReplicationConsumer();
        });
    }

    private synchronized void startNettyServer() {
        if (bossGroup != null || workerGroup != null) {
            log.warn("Netty server already started; skipping.");
            return;
        }

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new HttpServerCodec());
                            pipeline.addLast(new HttpObjectAggregator(65536));
                            pipeline.addLast(new ChunkedWriteHandler());
                            pipeline.addLast(new WebSocketServerProtocolHandler("/ws", null, true, 65536));
                            pipeline.addLast(new IdleStateHandler(30, 0, 0));
                            pipeline.addLast(new WebSocketFrameHandler());
                        }
                    });

            ChannelFuture f = b.bind(bindAddress, listenPort).sync();
            serverChannel = f.channel();
            log.info("Netty WebSocket Server started on {}:{}", bindAddress, listenPort);

            shutdownBarrier = new ShutdownSignalBarrier();
        } catch (InterruptedException e) {
            log.error("Interrupted while starting Netty server", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error while starting Netty server", e);
        }
    }

    private synchronized void stopNettyServer() {
        log.info("stopNettyServer. Started");
        try {
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while closing server channel", e);
        } finally {
            serverChannel = null;
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }

        if (shutdownBarrier != null) {
            shutdownBarrier.signal();
            shutdownBarrier = null;
        }

        log.info("stopNettyServer. Done");
    }

    private synchronized void startReplicationConsumer() {
        replicationConsumer = new ReplicationConsumer(clientRequests, appState);
        replicationConsumer.run();
    }

    private synchronized boolean stopReplicationConsumer() {
        log.info("stopReplicationConsumer. Started");
        if (replicationConsumer == null) {
            return false;
        }
        replicationConsumer.shutdown();
        this.seqNum = replicationConsumer.getLastSeqNum();
        replicationConsumer = null;
        log.info("stopReplicationConsumer. Done");
        return true;
    }

    private synchronized void startRequestSequencer() {
        requestSequencer = new RequestSequencer(clientRequests, seqNum, appState);
        requestSequencerThread = new Thread(requestSequencer, "RequestSequencerThread");
        requestSequencerThread.start();
    }

    private synchronized void stopRequestSequencer() {
        log.info("stopRequestSequencer. Started");
        if (requestSequencer != null) {
            requestSequencer.shutdown();
        }
        if (requestSequencerThread != null && requestSequencerThread.isAlive()) {
            requestSequencerThread.interrupt();
            try {
                requestSequencerThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        requestSequencerThread = null;
        requestSequencer = null;
        log.info("stopRequestSequencer. Done");
    }

    public synchronized void shutdown() {
        log.info("shutdown. Started");
        stopNettyServer();
        stopRequestSequencer();
        log.info("shutdown. Done");
    }

    private void processResponse(OrderMessage orderMessage) {
        try {
            if (appState.isNotRecoveredLeader()) {
                log.debug("Not Publishing {}", orderMessage);
                return;
            }
            if (orderMessage == null) {
                log.error("processResponse. Received null response");
                return;
            }
//            log.info("processResponse. {}", orderMessage);

            long seq = respSeqNum.getAndIncrement();
            orderMessage.setSeqNum(seq);

            long clientId = orderMessage.getClientId();
            Channel channel = channelsByClientId.get(clientId);
            if (channel == null) {
                log.error("processResponse. Client channel not found for clientId={}", clientId);
                return;
            }

            ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
            int serializedLength = OrderMessageSerDe.serialize(orderMessage, buffer, 0);

            byte[] bytes = new byte[serializedLength];
            buffer.getBytes(0, bytes);
            ByteBuf msg = Unpooled.copiedBuffer(bytes);

            int attempt = 0;
            while (true) {
                try {
                    ChannelFuture future = channel.writeAndFlush(new BinaryWebSocketFrame(msg.retainedDuplicate())).sync();
                    if (future.isSuccess()) {
                        break;
                    } else {
                        attempt++;
                        Thread.sleep(attempt);
                    }
                } catch (InterruptedException e) {
                    log.error("Error while trying to pause between retries", e);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Failed to send response {}, attempt={}", orderMessage, attempt, e);
                    attempt++;
                    try {
                        Thread.sleep(attempt);
                    } catch (InterruptedException ie) {
                        log.error("Interrupted while retry-sleeping", ie);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            log.info("Sent {}", orderMessage);
        } catch (Exception e) {
            log.error("processResponse. Error processing response", e);
        }

    }

    private class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state() == IdleState.READER_IDLE) {
                    log.warn("Server: Closing idle connection {}", ctx.channel().id());
                    ctx.close();
                }
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log.info("Session {} connected", ctx.channel().id().asShortText());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("Session {} disconnected", ctx.channel().id().asShortText());
            ChannelId channelId = ctx.channel().id();
            Long clientId = clientIdByChannelId.remove(channelId);
            if (clientId != null) {
                channelsByClientId.remove(clientId);
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            log.info("Received frame: {}", frame != null ? frame.getClass().getSimpleName(): null);

            if (frame instanceof CloseWebSocketFrame) {
                ctx.close();
                return;
            }

            if (frame instanceof PingWebSocketFrame) {
                ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                return;
            }
            if (frame instanceof PongWebSocketFrame) {
                return;
            }
            if (frame instanceof TextWebSocketFrame) {
                log.debug("Ignoring text frame: {}", ((TextWebSocketFrame) frame).text());
                return;
            }

            if (frame instanceof BinaryWebSocketFrame) {
                try {
                    ByteBuf content = frame.content();
                    int length = content.readableBytes();
                    byte[] data = new byte[length];
                    content.readBytes(data);
                    DirectBuffer directBuffer = new UnsafeBuffer(data);

                    OrderRequest orderRequest = OrderRequestSerDe.deserializeClientRequest(directBuffer, 0, length);
                    long clientId = orderRequest.getClientId();

                    if (!channelsByClientId.containsKey(clientId)) {
                        log.info("First request from clientId={}", clientId);
                        channelsByClientId.put(clientId, ctx.channel());
                        clientIdByChannelId.put(ctx.channel().id(), clientId);
                    }

                    if (requestSequencer != null) {
                        requestSequencer.process(orderRequest);
                    } else {
                        log.warn("RequestSequencer not active, ignoring request from clientId={}", clientId);
                    }

                } catch (Exception e) {
                    log.error("Error decoding BinaryWebSocketFrame", e);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Exception in WebSocketFrameHandler. Closing channel {}", ctx.channel().id().asShortText(), cause);
            ctx.close();
        }
    }
}