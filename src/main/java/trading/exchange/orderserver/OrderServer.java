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
import trading.exchange.LeadershipManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Netty-based implementation of what was previously a Babl-based WebSocket server.
 * Listens on bindAddress:listenPort for incoming WebSocket connections,
 * decodes binary frames into OrderRequests, and enqueues them.
 * Also sends OrderMessage responses to connected clients.
 */
public class OrderServer {
    private static final Logger log = LoggerFactory.getLogger(OrderServer.class);

    private final LFQueue<OrderRequest> clientRequests;
    private final LeadershipManager leadershipManager;

    private ReplicationConsumer replicationConsumer;
    private RequestSequencer requestSequencer;

    private final AtomicLong respSeqNum = new AtomicLong(1);

    // Store clientId -> Channel and ChannelId -> clientId
    private final Map<Long, Channel> channelsByClientId = new ConcurrentHashMap<>();
    private final Map<ChannelId, Long> clientIdByChannelId = new ConcurrentHashMap<>();

    private final String bindAddress;
    private final int listenPort;

    private Thread requestSequencerThread;

    // Netty groups and server channel
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    private ShutdownSignalBarrier shutdownBarrier;

    /**
     * Constructor.
     * @param clientRequests  The queue where client OrderRequests are placed
     * @param clientResponses   The queue from which OrderMessages will be read and sent to clients
     * @param leadershipManager Manages whether this server is leader or follower
     */
    public OrderServer(LFQueue<OrderRequest> clientRequests,
                       LFQueue<OrderMessage> clientResponses,
                       LeadershipManager leadershipManager) {
        this.clientRequests = clientRequests;
        this.leadershipManager = leadershipManager;

        this.bindAddress = Utils.env("WS_IP", "0.0.0.0");
        this.listenPort = Integer.parseInt(Utils.env("WS_PORT", "8080"));

        // Subscribe to responses; process them in processResponse method
        clientResponses.subscribe(this::processResponse);
    }

    /**
     * Start the server. Leadership changes will start/stop the Netty server
     * and replication consumer accordingly.
     */
    public synchronized void start() {
        leadershipManager.onLeadershipAcquired(() -> {
            stopReplicationConsumer();
            startRequestSequencer();
            startNettyServer();
        });

        leadershipManager.onLeadershipLost(() -> {
            stopNettyServer();
            stopRequestSequencer();
            startReplicationConsumer();
        });
    }

    /**
     * Start the Netty WebSocket server.
     */
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
                            // Standard HTTP handlers
                            pipeline.addLast(new HttpServerCodec());
                            pipeline.addLast(new HttpObjectAggregator(65536));
                            pipeline.addLast(new ChunkedWriteHandler());
                            // WebSocket upgrade handler
                            pipeline.addLast(new WebSocketServerProtocolHandler("/ws", null, true, 65536));
                            // Custom handler to manage frames
                            pipeline.addLast(new WebSocketFrameHandler());
                        }
                    });

            ChannelFuture f = b.bind(bindAddress, listenPort).sync();
            serverChannel = f.channel();
            log.info("Netty WebSocket Server started on {}:{}", bindAddress, listenPort);

            // We use a ShutdownSignalBarrier to coordinate stopping
            shutdownBarrier = new ShutdownSignalBarrier();
            // You could block here, but typically you'd let leadership
            // control or keep the thread active until you need to shut down.
            // If you'd like to block in this method, uncomment:
            // shutdownBarrier.await();

        } catch (InterruptedException e) {
            log.error("Interrupted while starting Netty server", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error while starting Netty server", e);
        }
    }

    /**
     * Stop the Netty WebSocket server.
     */
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

        // Signal any waiting threads (if we used barrier.await())
        if (shutdownBarrier != null) {
            shutdownBarrier.signal();
            shutdownBarrier = null;
        }

        log.info("stopNettyServer. Done");
    }

    private synchronized void startReplicationConsumer() {
        replicationConsumer = new ReplicationConsumer(clientRequests);
        replicationConsumer.run();
    }

    private synchronized void stopReplicationConsumer() {
        log.info("stopReplicationConsumer. Started");
        if (replicationConsumer != null) {
            replicationConsumer.shutdown();
        }
        replicationConsumer = null;
        log.info("stopReplicationConsumer. Done");
    }

    private synchronized void startRequestSequencer() {
        requestSequencer = new RequestSequencer(clientRequests);
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

    /**
     * Shutdown the server completely (if it's leader). Typically used for a graceful shutdown.
     */
    public synchronized void shutdown() {
        log.info("shutdown. Started");
        stopNettyServer();
        stopRequestSequencer();
        log.info("shutdown. Done");
    }

    /**
     * Called when a new OrderMessage is ready to be sent to the client.
     */
    private void processResponse(OrderMessage orderMessage) {
        try {
            if (leadershipManager.isFollower()) {
                log.info("Not Publishing {}", orderMessage);
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

            // Indefinite retry loop, mimicking the old code's approach.
            // This is NOT recommended in Netty's event loop, so be aware of potential blocking.
            byte[] bytes = new byte[serializedLength];
            buffer.getBytes(0, bytes);  // Always safe, regardless of internal backing
            ByteBuf msg = Unpooled.copiedBuffer(bytes);

            int attempt = 0;
            while (true) {
                try {
                    ChannelFuture future = channel.writeAndFlush(new BinaryWebSocketFrame(msg.retainedDuplicate())).sync();
                    if (future.isSuccess()) {
                        break; // Successfully sent
                    } else {
                        // Sleep a bit before retry
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

            log.info("Sent response {}. OK", orderMessage);
        } catch (Exception e) {
            log.error("processResponse. Error processing response", e);
        }

    }

    /**
     * Handler for inbound WebSocket frames.
     * Decodes BinaryWebSocketFrame -> OrderRequest, then hands off to RequestSequencer.
     */
    private class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

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
            if (frame instanceof CloseWebSocketFrame) {
                ctx.close();
                return;
            }
            if (frame instanceof PingWebSocketFrame) {
                ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                return;
            }
            if (frame instanceof PongWebSocketFrame) {
                // ignore pongs
                return;
            }
            if (frame instanceof TextWebSocketFrame) {
                // We only expect binary frames, but we can log or ignore text
                log.debug("Ignoring text frame: {}", ((TextWebSocketFrame) frame).text());
                return;
            }

            // Must be a BinaryWebSocketFrame
            if (frame instanceof BinaryWebSocketFrame) {
                ByteBuf content = ((BinaryWebSocketFrame) frame).content();
                int length = content.readableBytes();
                byte[] data = new byte[length];
                content.readBytes(data);
                DirectBuffer directBuffer = new UnsafeBuffer(data);

                try {
                    OrderRequest orderRequest = OrderRequestSerDe.deserializeClientRequest(directBuffer, 0, length);
                    long clientId = orderRequest.getClientId();

                    // If first request from this client, store channel
                    if (!channelsByClientId.containsKey(clientId)) {
                        log.info("First request from clientId={}", clientId);
                        channelsByClientId.put(clientId, ctx.channel());
                        clientIdByChannelId.put(ctx.channel().id(), clientId);
                    }

                    // Hand off to the RequestSequencer
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