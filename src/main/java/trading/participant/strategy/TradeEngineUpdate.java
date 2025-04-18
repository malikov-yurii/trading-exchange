package trading.participant.strategy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;

@Getter
@Setter
@NoArgsConstructor
public class TradeEngineUpdate {

    private final MarketUpdate marketUpdate = new MarketUpdate();
    private final OrderMessage orderMessage = new OrderMessage();
    private Type type;


    public TradeEngineUpdate(Type type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "TE-Upd{" + (isMarketData() ? marketUpdate : orderMessage +"}");
    }

    private boolean isMarketData() {
        return type == Type.MARKET_UPDATE;
    }

    public static void copy(TradeEngineUpdate from, TradeEngineUpdate to) {
        to.type = from.type;
        if (from.isMarketData()) {
            MarketUpdate.copy(from.marketUpdate, to.marketUpdate);
        } else {
            OrderMessage.copy(from.orderMessage, to.orderMessage);
        }
    }

    public void set(MarketUpdate marketUpdate) {
        this.type = Type.MARKET_UPDATE;
        MarketUpdate.copy(marketUpdate, this.marketUpdate);
    }

    public void set(OrderMessage orderMessage) {
        this.type = Type.ORDER_MESSAGE;
        OrderMessage.copy(orderMessage, this.orderMessage);
    }

    public enum Type {
        MARKET_UPDATE,
        ORDER_MESSAGE
    }

}
