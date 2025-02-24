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

        MarketDataConsumer marketDataConsumer = new MarketDataConsumer();
        Thread marketDataConsumerThread = new Thread(marketDataConsumer);
        marketDataConsumerThread.start();

        MarketDataSnapshotConsumer marketDataSnapshotConsumer = new MarketDataSnapshotConsumer();
        Thread marketDataSnapshotConsumerThread = new Thread(marketDataSnapshotConsumer);
        marketDataSnapshotConsumerThread.start();

        Thread.sleep(200); // TODO Improve waiting for MarketDataConsumer to start

        OrderGatewayClient orderGatewayClient = new OrderGatewayClient(uri);
        orderGatewayClient.start();

        new ShutdownSignalBarrier().await();
        orderGatewayClient.shutdown();
        marketDataConsumer.shutdown();
        marketDataSnapshotConsumer.shutdown();
        log.info("ParticipantApplication terminated.");

    }
}