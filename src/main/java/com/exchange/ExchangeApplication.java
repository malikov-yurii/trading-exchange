package com.exchange;

import com.exchange.marketdata.MarketUpdate;
import com.exchange.matching.MatchingEngine;
import com.exchange.orderserver.BablOrderServer;
import com.exchange.orderserver.ClientRequest;
import com.exchange.orderserver.ClientResponse;
import com.exchange.orderserver.DisruptorLFQueue;
import com.exchange.orderserver.LFQueue;

public class ExchangeApplication {

    public static void main(String[] args) {
        System.out.println("Trading Exchange Application Starting...");
        LFQueue<ClientRequest> clientRequests = new DisruptorLFQueue<>(1024, "clientRequests");
        LFQueue<ClientResponse> clientResponses = new DisruptorLFQueue<>(1024, "clientResponses");
        LFQueue<MarketUpdate> marketUpdates = new DisruptorLFQueue<>(1024, "marketUpdates");

        MatchingEngine matchingEngine = new MatchingEngine(clientRequests, clientResponses, marketUpdates);
        BablOrderServer bablOrderServer = new BablOrderServer(clientRequests, clientResponses);

        clientRequests.init();
        clientResponses.init();
        marketUpdates.init();

        matchingEngine.start();
        bablOrderServer.start();
        System.out.println("Trading Exchange Application Started");
    }

}