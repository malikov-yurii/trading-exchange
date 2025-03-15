package trading.participant.strategy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;

@Getter
@Setter
@AllArgsConstructor
public class TradeEngineUpdate {
    private MarketUpdate marketUpdate;
    private OrderMessage orderMessage;

    @Override
    public String toString() {
        return "TE-Upd{" +
                (marketUpdate != null ? marketUpdate.toString() : "") +
                (orderMessage != null ? orderMessage.toString() : "") +
        '}';
    }
}
