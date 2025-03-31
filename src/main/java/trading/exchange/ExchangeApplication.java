package trading.exchange;

import com.lmax.disruptor.dsl.ProducerType;
import trading.common.Constants;
import trading.common.Utils;
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

import java.net.UnknownHostException;

public class ExchangeApplication {
    private static final Logger log = LoggerFactory.getLogger(ExchangeApplication.class);
    private LFQueue<OrderRequest> clientRequests;
    private LFQueue<OrderMessage> clientResponses;
    private LFQueue<MarketUpdate> marketUpdates;
    private LFQueue<MarketUpdate> sequencedMarketUpdates;
    private MatchingEngine matchingEngine;
    private MarketDataSnapshotPublisher snapshotPublisher;
    private MarketDataPublisher marketDataPublisher;
    private OrderServer orderServer;

    public static void main(String[] args) throws UnknownHostException {
        ExchangeApplication exchangeApplication = new ExchangeApplication();

        ZooKeeperLeadershipManager leadershipManager = new ZooKeeperLeadershipManager();

        leadershipManager.onLeadershipAcquired(() -> {
            log.info("onLeadershipAcquired. Starting the exchange");
            exchangeApplication.startExchange();
        });

        leadershipManager.onLeadershipLost(() -> {
            log.info("onLeadershipLost. Shutting down the exchange");
            exchangeApplication.shutdownExchange();
        });

        leadershipManager.start();

        new ShutdownSignalBarrier().await();
        exchangeApplication.shutdownExchange();
    }

    private synchronized void startExchange() {
        log.info("startExchange. Starting.");
        clientRequests = new DisruptorLFQueue<>(1024, "clientRequests", ProducerType.SINGLE);
        clientResponses = new DisruptorLFQueue<>(1024, "clientResponses", ProducerType.SINGLE);
        marketUpdates = new DisruptorLFQueue<>(1024, "marketUpdates", ProducerType.SINGLE);
        sequencedMarketUpdates = new DisruptorLFQueue<>(1024, "sequencedMarketUpdates", ProducerType.SINGLE);

        matchingEngine = new MatchingEngine(clientRequests, clientResponses, marketUpdates);

        orderServer = new OrderServer(clientRequests, clientResponses,
                Utils.env("WS_IP", "0.0.0.0"),
                Integer.valueOf(Utils.env("WS_PORT", "8080")));

        String mdIp = Utils.env("MD_IP", "224.0.1.1");
        marketDataPublisher = new MarketDataPublisher(marketUpdates, sequencedMarketUpdates,
                "aeron:udp?endpoint=" + mdIp + ":" + Utils.env("MD_PORT", "40456"), 1001);
        snapshotPublisher = new MarketDataSnapshotPublisher(sequencedMarketUpdates, Constants.ME_MAX_TICKERS,
                "aeron:udp?endpoint=" + mdIp + ":" + Utils.env("MD_SNAPSHOT_PORT", "40457"), 2001);

        clientRequests.init();
        clientResponses.init();
        marketUpdates.init();
        sequencedMarketUpdates.init();

        matchingEngine.start();
        orderServer.start();
        log.info("startExchange. Started.");
    }

    private synchronized void shutdownExchange() {
        snapshotPublisher.close();
        clientRequests.shutdown();
        clientResponses.shutdown();
        marketUpdates.shutdown();
        sequencedMarketUpdates.shutdown();
        log.info("Trading Exchange Application terminated");
        System.exit(0);
    }

}