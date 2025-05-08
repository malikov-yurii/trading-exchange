package trading.participant.strategy;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import trading.api.MarketUpdate;
import trading.api.OrderResponse;

@Getter
@Setter
@NoArgsConstructor
public class TradeEngineUpdate {

    private final MarketUpdate marketUpdate = new MarketUpdate();
    private final OrderResponse orderResponse = new OrderResponse();
    private Type type;


    public TradeEngineUpdate(Type type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "TE-Upd{" + (isMarketData() ? marketUpdate : orderResponse +"}");
    }

    private boolean isMarketData() {
        return type == Type.MARKET_UPDATE;
    }

    public static void copy(TradeEngineUpdate from, TradeEngineUpdate to) {
        to.type = from.type;
        if (from.isMarketData()) {
            MarketUpdate.copy(from.marketUpdate, to.marketUpdate);
        } else {
            OrderResponse.copy(from.orderResponse, to.orderResponse);
        }
    }

    public void set(MarketUpdate marketUpdate) {
        this.type = Type.MARKET_UPDATE;
        MarketUpdate.copy(marketUpdate, this.marketUpdate);
    }

    public void set(OrderResponse orderResponse) {
        this.type = Type.ORDER_MESSAGE;
        OrderResponse.copy(orderResponse, this.orderResponse);
    }

    public enum Type {
        MARKET_UPDATE,
        ORDER_MESSAGE
    }

}
