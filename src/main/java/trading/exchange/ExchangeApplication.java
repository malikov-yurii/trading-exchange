package trading.exchange;

import com.lmax.disruptor.dsl.ProducerType;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderResponse;
import trading.api.OrderRequest;
import trading.common.Constants;
import trading.common.DisruptorLFQueue;
import trading.common.DisruptorLogger;
import trading.common.LFQueue;
import trading.exchange.marketdata.MarketDataPublisher;
import trading.exchange.marketdata.MarketDataSnapshotPublisher;
import trading.exchange.matching.MatchingEngine;
import trading.exchange.orderserver.FIXOrderServer;
import trading.exchange.orderserver.OrderServer;

import java.net.UnknownHostException;

public class ExchangeApplication {
    private static final Logger log = LoggerFactory.getLogger(ExchangeApplication.class);

    private ZooKeeperLeadershipManager leadershipManager;

    private LFQueue<OrderRequest> clientRequests;
    private LFQueue<OrderResponse> clientResponses;
    private LFQueue<MarketUpdate> marketUpdates;
    private LFQueue<MarketUpdate> sequencedMarketUpdates;
    private MatchingEngine matchingEngine;
    private MarketDataSnapshotPublisher snapshotPublisher;
    private MarketDataPublisher marketDataPublisher;
    private OrderServer orderServer;
    private AppState appState;
    private DisruptorLogger asyncLogger;

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
        appState = new AppState(leadershipManager);

        asyncLogger = new DisruptorLogger(10);

        clientRequests = new DisruptorLFQueue<>(null, "clientRequests", ProducerType.SINGLE, OrderRequest::new, OrderRequest::copy);
        clientResponses = new DisruptorLFQueue<>(null, "clientResponses", ProducerType.SINGLE, OrderResponse::new, OrderResponse::copy);
        marketUpdates = new DisruptorLFQueue<>(20, "marketUpdates", ProducerType.SINGLE, MarketUpdate::new, MarketUpdate::copy);
        sequencedMarketUpdates = new DisruptorLFQueue<>(20, "sequencedMarketUpdates", ProducerType.SINGLE, MarketUpdate::new, MarketUpdate::copy);


        matchingEngine = new MatchingEngine(clientRequests, clientResponses, marketUpdates);
        orderServer = new FIXOrderServer(clientRequests, clientResponses, leadershipManager, appState, asyncLogger);
        marketDataPublisher = new MarketDataPublisher(marketUpdates, sequencedMarketUpdates, appState, asyncLogger);
        snapshotPublisher = new MarketDataSnapshotPublisher(sequencedMarketUpdates, appState, Constants.ME_MAX_TICKERS, asyncLogger);

        asyncLogger.init();
        clientRequests.init();
        clientResponses.init();
        marketUpdates.init();
        sequencedMarketUpdates.init();

        matchingEngine.start();
        orderServer.start();
        log.info("startExchange. Started.");
    }

    private synchronized void shutdownExchange() {
        orderServer.shutdown();
        marketDataPublisher.close();
        snapshotPublisher.close();
        clientRequests.shutdown();
        clientResponses.shutdown();
        marketUpdates.shutdown();
        sequencedMarketUpdates.shutdown();
        leadershipManager.shutdown();
        log.info("Trading Exchange Application terminated");

        asyncLogger.shutdown();
        matchingEngine.close();

        System.exit(0);
    }

}