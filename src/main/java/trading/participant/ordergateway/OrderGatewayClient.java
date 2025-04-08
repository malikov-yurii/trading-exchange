package trading.participant.ordergateway;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.agrona.ExpandableDirectByteBuffer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderMessage;
import trading.api.OrderMessageSerDe;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.LFQueue;
import trading.participant.strategy.TradeEngineUpdate;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class OrderGatewayClient {

    private static final Logger log = LoggerFactory.getLogger(OrderGatewayClient.class);

    private final List<String> orderServerUris;
    private final LFQueue<TradeEngineUpdate> tradeEngineUpdates;

    private volatile Integer currentConnectionServerId = null;

    private EventLoopGroup group;
    private Channel channel;

    private final AtomicLong orderSeqNum = new AtomicLong(1);
    private volatile boolean connecting = false;

    public OrderGatewayClient(
            String orderServerHosts, Integer orderServerPort,
            LFQueue<OrderRequest> orderRequests,
            LFQueue<TradeEngineUpdate> tradeEngineUpdates) {

        List<String> orderServerUris = Stream.of(orderServerHosts.split(","))
                .peek(host -> {
                    if (StringUtils.isBlank(host)) {
                        log.error("ORDER_SERVER_HOSTS env var is not valid");
                        throw new IllegalArgumentException("ORDER_SERVER_HOSTS env var is not valid");
                    }
                })
                .map(host -> "ws://" + host + ":" + orderServerPort + "/ws")
                .toList();

        log.info("Creating OrderGatewayClient. orderServerUris={}", orderServerUris);
        this.orderServerUris = orderServerUris;
        this.tradeEngineUpdates = tradeEngineUpdates;
        orderRequests.subscribe(this::doSendOrderRequest);
    }

    private void doSendOrderRequest(OrderRequest orderRequest) {
        waitActiveChannel();
        try {
            long seq = orderSeqNum.getAndIncrement();
            orderRequest.setSeqNum(seq);

            ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);

            int len = OrderRequestSerDe.serialize(orderRequest, buffer, 0);

            byte[] data = new byte[len];

            buffer.getBytes(0, data);

            ByteBuf nettyBuf = channel.alloc().buffer(data.length);
            nettyBuf.writeBytes(data);

            BinaryWebSocketFrame frame = new BinaryWebSocketFrame(nettyBuf);
            ChannelFuture channelFuture = channel.writeAndFlush(frame);
            log.info("Sent orderRequest seq={} ticker={} side={} to {}", seq, orderRequest.getTickerId(),
                    orderRequest.getSide(), currentConnectionServerId);
        } catch (Exception e) {
            log.error("Failed to send orderRequest", e);
        }
    }

    private void waitActiveChannel() {
        while (channel == null || !channel.isActive()) {
            try {
                log.info("Waiting 1s for active channel...");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    public synchronized void start() {
        if (group != null) {
            log.warn("Already started? skipping...");
            return;
        }

        group = new NioEventLoopGroup(1);
        connectPreferPrimary();
    }

    private synchronized void connectPreferPrimary() {
        if (currentConnectionServerId != null && channel != null && channel.isActive()) {
            return;
        }

        for (int serverId = 0; serverId < orderServerUris.size(); serverId++) {
            if (tryConnect(serverId)) {
                currentConnectionServerId = serverId;
                return;
            }
        }

        currentConnectionServerId = null;

        log.warn("Both primary and backup failed. Will retry in 2 seconds...");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // Ignore
        }
        connectPreferPrimary();
    }

//    TODO: try re-connect not more than once 100ms
    private synchronized boolean tryConnect(Integer serverId) {
        try {
            if (connecting) {
                log.warn("Already connecting. Skipping...");
                return false;
            }
            connecting = true;
            Channel oldChannel = channel;
            channel = null;
            if (oldChannel != null && oldChannel.isActive()) {
                oldChannel.close().sync();
            }

            String serverUri = orderServerUris.get(serverId);
            URI uri = new URI(serverUri);
            boolean ssl = "wss".equalsIgnoreCase(uri.getScheme());
            SslContext sslCtx = ssl ? buildSslContext() : null;

            WebSocketClientHandler handler = new WebSocketClientHandler(uri, serverUri);
            Bootstrap b = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            if (sslCtx != null) {
                                String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
                                int port = getPort(uri);
                                pipeline.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                            }
                            pipeline.addLast(new HttpClientCodec());
                            pipeline.addLast(new HttpObjectAggregator(8192));
                            pipeline.addLast(handler);
                        }
                    });

            String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
            int port = getPort(uri);
//            log.info("Attempting connect to {} at {}:{}", serverUri, host, port);

            ChannelFuture cf = b.connect(host, port);
            cf.addListener(f -> { /* Have to use listeners to not block Netty main loop */
                if (f.isSuccess()) {
                    handler.handshakeFuture().addListener(f2 -> { /* Have to use listeners to not block Netty main loop */
                        if (f2.isSuccess()) {
                            log.info("Handshake complete for {}", serverUri);
                            connecting = false;
                            channel = cf.channel();
                        } else {
                            log.warn("Handshake failed for {} -> {}", serverUri, f2.cause().getMessage());
                            connecting = false;
                            tryConnectToAnotherServer(serverId);
                        }
                    });
                    log.info("Connected to {} at {}:{}", serverUri, host, port);
                } else {
//                    log.warn("Failed to connect to {} -> {}", serverUri, f.cause().getMessage());
                    connecting = false;
                    tryConnectToAnotherServer(serverId);
                }
            });
            return true;
        } catch (Exception e) {
//            log.warn("Failed to connect to {} -> {}", serverUri, e.getMessage());
            connecting = false;
            return false;
        }
    }

    private void tryConnectToAnotherServer(int serverId) {
        int anotherServerid = serverId + 1 % orderServerUris.size();
        tryConnect(anotherServerid);
    }

    private int getPort(URI uri) {
        if (uri.getPort() != -1) {
            return uri.getPort();
        }
        return "wss".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    private SslContext buildSslContext() throws SSLException {
        return SslContextBuilder.forClient().build();
    }

    public synchronized void shutdown() {
        if (channel != null) {
            try {
                channel.close().sync();
            } catch (InterruptedException e) {
                log.warn("Interrupted closing channel", e);
            }
        }
        if (group != null) {
            group.shutdownGracefully();
            group = null;
        }
        log.info("OrderGatewayClient fully shut down");
    }

    private class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
        private final URI uri;
        private final String targetServer;
        private ChannelPromise handshakeFuture;
        private WebSocketClientHandshaker handshaker;

        WebSocketClientHandler(URI uri, String targetServer) {
            this.uri = uri;
            this.targetServer = targetServer;
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
            log.info("channelInactive. We lost connection to {}", targetServer);
            ctx.channel().eventLoop().submit(OrderGatewayClient.this::connectPreferPrimary);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (!handshaker.isHandshakeComplete()) {
                try {
                    handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                    log.info("WebSocket handshake complete for {}", targetServer);
                    handshakeFuture.setSuccess();
                } catch (Exception e) {
                    log.error("Handshake failed for {} -> {}", targetServer, e.getMessage());
                    handshakeFuture.setFailure(e);
                }
                return;
            }

            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException("Unexpected FullHttpResponse: " + response.status());
            } else if (msg instanceof TextWebSocketFrame) {
                TextWebSocketFrame textFrame = (TextWebSocketFrame) msg;
                log.info("Received Text frame from {}: {}", targetServer, textFrame.text());
            } else if (msg instanceof BinaryWebSocketFrame) {
                BinaryWebSocketFrame binFrame = (BinaryWebSocketFrame) msg;
                ByteBuf content = binFrame.content();
                ByteBuf leBuf = content.order(ByteOrder.LITTLE_ENDIAN); // match server endianness
                OrderMessage orderMsg = OrderMessageSerDe.parseOrderMessage(leBuf);
                onOrderMessage(orderMsg);
            } else if (msg instanceof PongWebSocketFrame) {
                log.info("Received Pong from {}", targetServer);
            } else if (msg instanceof CloseWebSocketFrame) {
                log.info("Received Close from {}", targetServer);
                ctx.channel().close();
            }
        }

        private void onOrderMessage(OrderMessage orderMessage) {
            log.info("Received {}", orderMessage);
            tradeEngineUpdates.offer(new TradeEngineUpdate(null, orderMessage));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("exceptionCaught in pipeline for {} -> {}", targetServer, cause.getMessage());
            ctx.close();
        }
    }
}