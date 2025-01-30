package com.exchange.matching;

import com.exchange.MEConstants;
import com.exchange.marketdata.MEMarketUpdate;
import com.exchange.marketdata.MEMarketUpdateLFQueue;
import com.exchange.orderserver.ClientRequestLFQueue;
import com.exchange.orderserver.ClientResponseLFQueue;
import com.exchange.orderserver.MEClientRequest;
import com.exchange.orderserver.MEClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public final class MatchingEngine {
    private static final Logger log = LoggerFactory.getLogger(MatchingEngine.class);

    private final MEOrderBook[] tickerOrderBook;
    private ClientRequestLFQueue incomingRequests;
    private ClientResponseLFQueue outgoingResponses;
    private MEMarketUpdateLFQueue outgoingMdUpdates;

    private volatile boolean run;
    private Thread engineThread;

    public MatchingEngine(
            ClientRequestLFQueue clientRequests,
            ClientResponseLFQueue clientResponses,
            MEMarketUpdateLFQueue marketUpdates
    ) {
        this.incomingRequests = clientRequests;
        this.outgoingResponses = clientResponses;
        this.outgoingMdUpdates = marketUpdates;
        this.tickerOrderBook = new MEOrderBook[MEConstants.ME_MAX_TICKERS];
        for (int i = 0; i < tickerOrderBook.length; i++) {
            this.tickerOrderBook[i] = new MEOrderBook(i, this);
        }
    }

    public void start() {
        run = true;
        engineThread = new Thread(this::run, "MatchingEngineMain");
        engineThread.start();
    }

    public void stop() {
        run = false;
    }

    public void close() {
        stop();

        try {
            engineThread.join(60_000);
        } catch (InterruptedException ignored) {
        }

        incomingRequests = null;
        outgoingResponses = null;
        outgoingMdUpdates = null;

        Arrays.fill(tickerOrderBook, null);
    }

    private void run() {
        log.info("MatchingEngine started");
        while (run) {
            MEClientRequest request = incomingRequests.poll();
            if (request != null) {
                log.info("Processing {}", request);
                processClientRequest(request);
            }
        }
        log.info("MatchingEngine stopped");
    }

    private void processClientRequest(MEClientRequest req) {
        MEOrderBook orderBook = tickerOrderBook[(int) req.getTickerId()];

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

    public void sendClientResponse(MEClientResponse response) {
        log.info("Sending {}", response);
        outgoingResponses.offer(response);
    }

    public void sendMarketUpdate(MEMarketUpdate update) {
        log.info("Sending {}", update);
        outgoingMdUpdates.offer(update);
    }

    private void fatal(String message) {
        throw new RuntimeException(message);
    }

}