package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.Side;
import trading.common.Constants;

public class LiquidityTaker implements TradingAlgo {
    private static final Logger log = LoggerFactory.getLogger(LiquidityTaker.class);

    private final FeatureEngine featureEngine;
    private final OrderManager orderManager;
    private final TradeEngineConfigMap tickerConfigMap;

    public LiquidityTaker(FeatureEngine featureEngine, OrderManager orderManager, TradeEngineConfigMap tickerConfigMap) {
        this.featureEngine = featureEngine;
        this.orderManager = orderManager;
        this.tickerConfigMap = tickerConfigMap;
    }

    @Override
    public void onOrderBookUpdate(long tickerId, long price, Side side, MarketOrderBook marketOrderBook) {
        log.info("LiquidityTaker received order book update: tickerId={}, price={}, side={}", tickerId, price, side);
    }

    @Override
    public void onTradeUpdate(MarketUpdate marketUpdate, MarketOrderBook marketOrderBook) {
        MarketOrderBook.BBO bbo = marketOrderBook.getBBO();
        double aggressiveTradeQtyRatio = featureEngine.getAggressiveTradeQtyRatio();

        if (bbo.getBidPrice() != Constants.PRICE_INVALID
                && bbo.getAskPrice() != Constants.PRICE_INVALID
                && aggressiveTradeQtyRatio != Constants.RATIO_INVALID) {

            long tickerId = marketUpdate.getTickerId();
            TradeEngineConfig tickerConfig = tickerConfigMap.get(tickerId);
            long threshold = tickerConfig.getThreshold();

            if (aggressiveTradeQtyRatio > threshold) {
                long orderQty = tickerConfig.getClip();
                if (marketUpdate.getSide() == Side.BUY) {
                    orderManager.moveOrders(tickerId, bbo.getAskPrice(), Constants.PRICE_INVALID, orderQty);
                } else {
                    orderManager.moveOrders(tickerId, Constants.PRICE_INVALID, bbo.getBidPrice(), orderQty);
                }
            }
        }
    }

    @Override
    public void onOrderUpdate(OrderMessage orderMessage) {
        orderManager.onOrderMessage(orderMessage);
    }

}
