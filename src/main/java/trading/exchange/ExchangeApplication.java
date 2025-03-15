package trading.exchange;

import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang3.ObjectUtils;
import trading.common.Constants;
import trading.exchange.marketdata.MarketDataPublisher;
import trading.api.MarketUpdate;
import trading.exchange.marketdata.MarketDataSnapshotPublisher;
import trading.exchange.matching.MatchingEngine;
import trading.exchange.orderserver.OrderServer;
import trading.api.OrderRequest;
import trading.api.OrderMessage;
import trading.common.DisruptorLFQueue;
import trading.common.LFQueue;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExchangeApplication {
    private static final Logger log = LoggerFactory.getLogger(ExchangeApplication.class);

    public static void main(String[] args) {
        System.out.println("Trading Exchange Application Starting...");
        LFQueue<OrderRequest> clientRequests = new DisruptorLFQueue<>(1024, "clientRequests", ProducerType.SINGLE);
        LFQueue<OrderMessage> clientResponses = new DisruptorLFQueue<>(1024, "clientResponses", ProducerType.SINGLE);
        LFQueue<MarketUpdate> marketUpdates = new DisruptorLFQueue<>(1024, "marketUpdates", ProducerType.SINGLE);
        LFQueue<MarketUpdate> sequencedMarketUpdates = new DisruptorLFQueue<>(1024, "sequencedMarketUpdates", ProducerType.SINGLE);

        MatchingEngine matchingEngine = new MatchingEngine(clientRequests, clientResponses, marketUpdates);

        OrderServer orderServer = new OrderServer(clientRequests, clientResponses,
                env("WS_IP", "0.0.0.0"),
                Integer.valueOf(env("WS_IP", "8080")));

        String mdIp = env("MD_IP", "224.0.1.1");
        MarketDataPublisher marketDataPublisher = new MarketDataPublisher(marketUpdates, sequencedMarketUpdates,
                "aeron:udp?endpoint=" + mdIp + ":" + env("MD_PORT", "40456"), 1001);
        MarketDataSnapshotPublisher snapshotPublisher = new MarketDataSnapshotPublisher(sequencedMarketUpdates, Constants.ME_MAX_TICKERS,
                "aeron:udp?endpoint=" + mdIp + ":" + env("MD_SNAPSHOT_PORT", "40457"), 2001);

        clientRequests.init();
        clientResponses.init();
        marketUpdates.init();
        sequencedMarketUpdates.init();

        matchingEngine.start();
        orderServer.start();
        log.info("Trading Exchange Application Started");

        new ShutdownSignalBarrier().await();
        snapshotPublisher.close();
        clientRequests.shutdown();
        clientResponses.shutdown();
        marketUpdates.shutdown();
        sequencedMarketUpdates.shutdown();
        log.info("Trading Exchange Application terminated");
        System.exit(0);
    }

    private static String env(String envVar, String defaultValue) {
        return ObjectUtils.defaultIfNull(System.getenv(envVar), defaultValue);
    }

}