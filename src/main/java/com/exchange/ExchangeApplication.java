package com.exchange;

import com.exchange.marketdata.MarketDataPublisher;
import com.exchange.api.MarketUpdate;
import com.exchange.matching.MatchingEngine;
import com.exchange.orderserver.BablOrderServer;
import com.exchange.api.OrderRequest;
import com.exchange.api.OrderMessage;
import com.exchange.orderserver.DisruptorLFQueue;
import com.exchange.orderserver.LFQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExchangeApplication {
    private static final Logger log = LoggerFactory.getLogger(ExchangeApplication.class);

    public static void main(String[] args) {
        System.out.println("Trading Exchange Application Starting...");
        LFQueue<OrderRequest> clientRequests = new DisruptorLFQueue<>(1024, "clientRequests");
        LFQueue<OrderMessage> clientResponses = new DisruptorLFQueue<>(1024, "clientResponses");
        LFQueue<MarketUpdate> marketUpdates = new DisruptorLFQueue<>(1024, "marketUpdates");

        MatchingEngine matchingEngine = new MatchingEngine(clientRequests, clientResponses, marketUpdates);
        BablOrderServer bablOrderServer = new BablOrderServer(clientRequests, clientResponses);
        MarketDataPublisher marketDataPublisher = new MarketDataPublisher(marketUpdates);

        clientRequests.init();
        clientResponses.init();
        marketUpdates.init();

        matchingEngine.start();
        bablOrderServer.start();
        log.info("Trading Exchange Application Started");
    }

}