package com.exchange.matching;

import com.exchange.MEConstants;
import com.exchange.marketdata.MarketUpdate;
import com.exchange.orderserver.ClientRequest;
import com.exchange.orderserver.ClientResponse;
import com.exchange.orderserver.LFQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public final class MatchingEngine {
    private static final Logger log = LoggerFactory.getLogger(MatchingEngine.class);

    private final OrderBook[] tickerOrderBook;
    private LFQueue<ClientRequest> incomingRequests;
    private LFQueue<ClientResponse> outgoingResponses;
    private LFQueue<MarketUpdate> outgoingMdUpdates;

    public MatchingEngine(
            LFQueue<ClientRequest> clientRequests,
            LFQueue<ClientResponse> clientResponses,
            LFQueue<MarketUpdate> marketUpdates
    ) {
        this.incomingRequests = clientRequests;
        this.outgoingResponses = clientResponses;
        this.outgoingMdUpdates = marketUpdates;
        this.tickerOrderBook = new OrderBook[MEConstants.ME_MAX_TICKERS];
        for (int i = 0; i < tickerOrderBook.length; i++) {
            this.tickerOrderBook[i] = new OrderBook(i, this);
        }
        incomingRequests.subscribe(this::processClientRequest);
    }

    public void start() {
        log.info("MatchingEngine started");
    }

    public void close() {
        incomingRequests = null;
        outgoingResponses = null;
        outgoingMdUpdates = null;

        Arrays.fill(tickerOrderBook, null);
    }

    private void processClientRequest(ClientRequest req) {
        log.info("Processing {}", req);
        OrderBook orderBook = tickerOrderBook[(int) req.getTickerId()];

        switch (req.getType()) {
            case NEW:
                orderBook.add(req.getClientId(), req.getOrderId(), req.getTickerId(), req.getSide(), req.getPrice(), req.getQty());
                break;
            case CANCEL:
                orderBook.cancel(req.getClientId(), req.getOrderId(), req.getTickerId());
                break;
            default:
                fatal("Received invalid client-request-type: " + req.getType());
        }
    }

    public void sendClientResponse(ClientResponse response) {
        log.info("Sending {}", response);
        outgoingResponses.offer(response);
    }

    public void sendMarketUpdate(MarketUpdate update) {
        log.info("Sending {}", update);
        outgoingMdUpdates.offer(update);
    }

    private void fatal(String message) {
        throw new RuntimeException(message);
    }

}