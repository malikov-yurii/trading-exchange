package trading.participant.strategy;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.OrderMessageType;
import trading.api.OrderRequest;
import trading.api.Side;
import trading.common.Constants;
import trading.common.LFQueue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

public class TradeEngine {
    private static final Logger log = LoggerFactory.getLogger(TradeEngine.class);

    private final LFQueue<OrderRequest> orderRequestQueue;

    private final FeatureEngine featureEngine;
    private final PositionManager positionManager;
    private final TradingAlgo algo;

    private final MarketOrderBook[] marketOrderBooks = new MarketOrderBook[Constants.ME_MAX_TICKERS];
    @Getter
    private final long clientId;

    private final AtomicLong lastUpdateTime = new AtomicLong();

    public TradeEngine(AlgoType algoType,
                       LFQueue<OrderRequest> orderRequestQueue,
                       LFQueue<TradeEngineUpdate> tradeEngineUpdateQueue,
                       long clientId) {
        this.orderRequestQueue = orderRequestQueue;

        this.clientId = clientId;
        tradeEngineUpdateQueue.subscribe(this::onTradeEngineUpdate);

        for (int i = 0; i < marketOrderBooks.length; i++) {
            marketOrderBooks[i] = new MarketOrderBook(i, this);
        }

        this.positionManager = new PositionManager();
        this.featureEngine = new FeatureEngine();

        TradeEngineConfigMap tradeEngineConfigMap = new TradeEngineConfigMap();
        RiskManager riskManager = new RiskManager(Constants.ME_MAX_TICKERS, positionManager, tradeEngineConfigMap);
        OrderManager orderManager = new OrderManager(this, riskManager);

        if (algoType == AlgoType.MARKET_MAKER) {
            this.algo = new MarketMaker(featureEngine, orderManager, tradeEngineConfigMap);
        } else if (algoType == AlgoType.LIQUIDITY_TAKER) {
            this.algo = new LiquidityTaker(featureEngine, orderManager, tradeEngineConfigMap);
        } else if (algoType == AlgoType.RANDOM) {
            this.algo = new RandomOrderSender(this);
        } else {
            throw new IllegalArgumentException("Unknown strategy type: " + algoType);
        }
    }

    public void start() {
        System.out.println("TradeEngine started");
    }

    public void sendOrderRequest(OrderRequest orderRequest) {
        log.info("sendOrderRequest. {}", orderRequest);
        orderRequestQueue.offer(orderRequest);
    }

    public void onTradeEngineUpdate(TradeEngineUpdate tradeEngineUpdate) {
        try {
            log.info("onTradeEngineUpdate. {}", tradeEngineUpdate);
            if (tradeEngineUpdate.getMarketUpdate() != null) {
                onMarketUpdate(tradeEngineUpdate.getMarketUpdate());
            } else if (tradeEngineUpdate.getOrderMessage() != null) {
                onOrderMessage(tradeEngineUpdate.getOrderMessage());
            }
        } catch (Throwable e) {
            log.error("Error processing TradeEngineUpdate: {}", tradeEngineUpdate, e);
        }
    }

    public void onMarketUpdate(MarketUpdate marketUpdate) {
        MarketOrderBook marketOrderBook = marketOrderBooks[(int) marketUpdate.getTickerId()];
        marketOrderBook.onMarketUpdate(marketUpdate);
        lastUpdateTime.set(System.currentTimeMillis());
    }

    public void onOrderMessage(OrderMessage orderMessage) {
        if (orderMessage.getType() == OrderMessageType.FILLED) {
            positionManager.addFill(orderMessage);
        }
        algo.onOrderUpdate(orderMessage);
        lastUpdateTime.set(System.currentTimeMillis());
    }

    public void onOrderBookUpdate(MarketOrderBook orderBook, long price, Side side) {
        try {
            log.info("onOrderBookUpdate. tickerId: {}, {} @{}", orderBook.getTickerId(), side, price);
            positionManager.updateBBO(orderBook.getTickerId(), orderBook.getBBO());
            featureEngine.onOrderBookUpdate(orderBook, price, side);
            algo.onOrderBookUpdate(orderBook.getTickerId(), price, side, orderBook);
        } catch (Exception e) {
            log.error("Error processing onOrderBookUpdate. tickerId: {}, price: {}, side: {}", orderBook.getTickerId(), price, side, e);
        }
    }

    public void onTradeUpdate(MarketOrderBook marketOrderBook, MarketUpdate tradeUpdate) {
        featureEngine.onTradeUpdate(marketOrderBook, tradeUpdate);
        algo.onTradeUpdate(tradeUpdate, marketOrderBook);
    }

    public LocalDateTime getLastUpdateTime() {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(lastUpdateTime.get()), ZoneId.systemDefault());
    }

    public void init() {
        algo.init();
    }

    public void shutdown() {
        algo.shutdown();
    }

}
