package com.exchange;

import com.exchange.marketdata.MarketDataPublisher;
import com.exchange.api.MarketUpdate;
import com.exchange.marketdata.MarketDataSnapshotPublisher;
import com.exchange.matching.MatchingEngine;
import com.exchange.orderserver.BablOrderServer;
import com.exchange.api.OrderRequest;
import com.exchange.api.OrderMessage;
import com.exchange.orderserver.DisruptorLFQueue;
import com.exchange.orderserver.LFQueue;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExchangeApplication {
    private static final Logger log = LoggerFactory.getLogger(ExchangeApplication.class);

    public static void main(String[] args) {
        System.out.println("Trading Exchange Application Starting...");
        LFQueue<OrderRequest> clientRequests = new DisruptorLFQueue<>(1024, "clientRequests");
        LFQueue<OrderMessage> clientResponses = new DisruptorLFQueue<>(1024, "clientResponses");
        LFQueue<MarketUpdate> marketUpdates = new DisruptorLFQueue<>(1024, "marketUpdates");
        LFQueue<MarketUpdate> sequencedMarketUpdates = new DisruptorLFQueue<>(1024, "sequencedMarketUpdates");

        MatchingEngine matchingEngine = new MatchingEngine(clientRequests, clientResponses, marketUpdates);
        BablOrderServer bablOrderServer = new BablOrderServer(clientRequests, clientResponses);
        MarketDataPublisher marketDataPublisher = new MarketDataPublisher(marketUpdates, sequencedMarketUpdates,
                "aeron:udp?endpoint=224.0.1.1:40456", 1001);
        MarketDataSnapshotPublisher snapshotPublisher = new MarketDataSnapshotPublisher(sequencedMarketUpdates, Constants.ME_MAX_TICKERS,
                "aeron:udp?endpoint=224.0.1.1:40457", 2001);

        clientRequests.init();
        clientResponses.init();
        marketUpdates.init();
        sequencedMarketUpdates.init();

        matchingEngine.start();
        bablOrderServer.start();
        log.info("Trading Exchange Application Started");

        new ShutdownSignalBarrier().await();
        snapshotPublisher.close();
    }

}