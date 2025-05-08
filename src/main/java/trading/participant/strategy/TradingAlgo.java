package trading.participant.strategy;

import trading.api.MarketUpdate;
import trading.api.OrderResponse;
import trading.api.Side;

public interface TradingAlgo {

    void onOrderBookUpdate(long tickerId, long price, Side side, MarketOrderBook marketOrderBook);
    void onTradeUpdate(MarketUpdate marketUpdate, MarketOrderBook marketOrderBook);
    void onOrderUpdate(OrderResponse orderResponse);
    default void init() {};
    default void shutdown() {}

}
