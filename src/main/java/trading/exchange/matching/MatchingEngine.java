package trading.exchange.matching;

import trading.common.Constants;
import trading.api.MarketUpdate;
import trading.api.OrderRequest;
import trading.api.OrderMessage;
import trading.common.LFQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public final class MatchingEngine {
    private static final Logger log = LoggerFactory.getLogger(MatchingEngine.class);

    private final OrderBook[] tickerOrderBook;
    private LFQueue<OrderRequest> incomingRequests;
    private LFQueue<OrderMessage> outgoingResponses;
    private LFQueue<MarketUpdate> outgoingMdUpdates;

    public MatchingEngine(
            LFQueue<OrderRequest> clientRequests,
            LFQueue<OrderMessage> clientResponses,
            LFQueue<MarketUpdate> marketUpdates
    ) {
        this.incomingRequests = clientRequests;
        this.outgoingResponses = clientResponses;
        this.outgoingMdUpdates = marketUpdates;
        this.tickerOrderBook = new OrderBook[Constants.ME_MAX_TICKERS];
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

    private void processClientRequest(OrderRequest req) {
        if (log.isDebugEnabled()) {
            log.debug("Processing {}", req);
        }
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

    public void sendClientResponse(OrderMessage response) {
        if (log.isDebugEnabled()) {
            log.debug("Sending {}", response);
        }
        outgoingResponses.offer(response);
    }

    public void sendMarketUpdate(MarketUpdate update) {
        if (log.isDebugEnabled()) {
            log.debug("Sending {}", update);
        }
        outgoingMdUpdates.offer(update);
    }

    private void fatal(String message) {
        throw new RuntimeException(message);
    }

}