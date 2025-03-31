package trading.exchange;

import com.lmax.disruptor.dsl.ProducerType;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.OrderRequest;
import trading.common.Constants;
import trading.common.DisruptorLFQueue;
import trading.common.LFQueue;
import trading.exchange.marketdata.MarketDataPublisher;
import trading.exchange.marketdata.MarketDataSnapshotPublisher;
import trading.exchange.matching.MatchingEngine;
import trading.exchange.orderserver.OrderServer;

import java.net.UnknownHostException;

public class ExchangeApplication {
    private static final Logger log = LoggerFactory.getLogger(ExchangeApplication.class);

    private ZooKeeperLeadershipManager leadershipManager;

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
        exchangeApplication.startExchange();

        new ShutdownSignalBarrier().await();
        exchangeApplication.shutdownExchange();
    }

    private synchronized void startExchange() throws UnknownHostException {
        log.info("startExchange. Starting.");
        leadershipManager = new ZooKeeperLeadershipManager();
        leadershipManager.start();

        clientRequests = new DisruptorLFQueue<>(1024, "clientRequests", ProducerType.SINGLE);
        clientResponses = new DisruptorLFQueue<>(1024, "clientResponses", ProducerType.SINGLE);
        marketUpdates = new DisruptorLFQueue<>(1024, "marketUpdates", ProducerType.SINGLE);
        sequencedMarketUpdates = new DisruptorLFQueue<>(1024, "sequencedMarketUpdates", ProducerType.SINGLE);

        matchingEngine = new MatchingEngine(clientRequests, clientResponses, marketUpdates);

        orderServer = new OrderServer(clientRequests, clientResponses, leadershipManager);
        marketDataPublisher = new MarketDataPublisher(marketUpdates, sequencedMarketUpdates, leadershipManager);
        snapshotPublisher = new MarketDataSnapshotPublisher(sequencedMarketUpdates, leadershipManager, Constants.ME_MAX_TICKERS);

        clientRequests.init();
        clientResponses.init();
        marketUpdates.init();
        sequencedMarketUpdates.init();

        matchingEngine.start();
        try {
            // todo check why does not work without it
            Thread.sleep(7_000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        orderServer.start();
        log.info("startExchange. Started.");
    }

    private synchronized void shutdownExchange() {
        snapshotPublisher.close();
        clientRequests.shutdown();
        clientResponses.shutdown();
        marketUpdates.shutdown();
        sequencedMarketUpdates.shutdown();
        leadershipManager.shutdown();
        log.info("Trading Exchange Application terminated");

        // TODO: Shutdown System.exit(0) on  shutdownExchange()???
        System.exit(0);
    }

}