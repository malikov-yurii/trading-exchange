package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderResponse;
import trading.api.Side;
import trading.common.Constants;

public class MarketMaker implements TradingAlgo {
    private static final Logger log = LoggerFactory.getLogger(MarketMaker.class);

    private final FeatureEngine featureEngine;
    private final OrderManager orderManager;
    private final TradeEngineConfigMap tickerConfigMap;

    public MarketMaker(FeatureEngine featureEngine, OrderManager orderManager, TradeEngineConfigMap tickerConfigMap) {
        log.info("TradingAlgo. Init.");
        this.featureEngine = featureEngine;
        this.orderManager = orderManager;
        this.tickerConfigMap = tickerConfigMap;
    }

    @Override
    public void onOrderBookUpdate(long tickerId, long price, Side side, MarketOrderBook marketOrderBook) {
//        log.info("MarketMaker received order book update: tickerId={}, price={}, side={}", tickerId, price, side);
        MarketOrderBook.BBO bbo = marketOrderBook.getBBO();
        double fairPrice = featureEngine.getFairMarketPrice();

        if (bbo.getBidPrice() != Constants.PRICE_INVALID
                && bbo.getAskPrice() != Constants.PRICE_INVALID
                && fairPrice != Constants.PRICE_INVALID) {

            TradeEngineConfig tickerConfig = tickerConfigMap.get(tickerId);
            long orderQty = tickerConfig.getClip();
            long threshold = tickerConfig.getThreshold();

            long bidPrice = bbo.getBidPrice() - (fairPrice - bbo.getBidPrice() < threshold ? 1 : 0);
            long askPrice = bbo.getAskPrice() + (bbo.getAskPrice() - fairPrice < threshold ? 1 : 0);

            orderManager.moveOrders(tickerId, bidPrice, askPrice, orderQty);
        }
    }

    @Override
    public void onTradeUpdate(MarketUpdate marketUpdate, MarketOrderBook marketOrderBook) {
//        log.info("MarketMaker received trade update: {}", marketUpdate);
    }

    @Override
    public void onOrderUpdate(OrderResponse orderResponse) {
//        log.info("onOrderUpdate. {}", orderMessage);
        orderManager.onOrderMessage(orderResponse);
    }

}
