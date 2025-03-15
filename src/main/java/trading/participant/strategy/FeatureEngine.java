package trading.participant.strategy;

import lombok.Getter;
import trading.api.MarketUpdate;
import trading.api.Side;
import trading.common.Constants;

@Getter
public class FeatureEngine {

    private double marketPrice = Constants.PRICE_INVALID;
    private double aggressiveTradeQtyRatio = Constants.RATIO_INVALID;

    public void onOrderBookUpdate(MarketOrderBook orderBook, long price, Side side) {
        MarketOrderBook.BBO bbo = orderBook.getBBO();
        long bidPrice = bbo.getBidPrice();
        long askPrice = bbo.getAskPrice();
        if (bidPrice != Constants.PRICE_INVALID && askPrice != Constants.PRICE_INVALID) {
            long askQty = bbo.getAskQty();
            long bidQty = bbo.getBidQty();
            marketPrice = ((double) (bidPrice * askQty + askPrice * bidQty)) / (askQty + bidQty);
        }
    }

    public void onTradeUpdate(MarketOrderBook orderBook, MarketUpdate tradeUpdate) {
        MarketOrderBook.BBO bbo = orderBook.getBBO();
        if (bbo.getBidPrice() != Constants.PRICE_INVALID && bbo.getAskPrice() != Constants.PRICE_INVALID) {
            aggressiveTradeQtyRatio = ((double) tradeUpdate.getQty())
                    / (tradeUpdate.getSide() == Side.BUY ? bbo.getAskQty() : bbo.getBidQty());
        }
    }

}
