package trading.participant.strategy;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderMessage;
import trading.api.Side;

import static trading.common.Constants.PRICE_INVALID;

@Getter
@Setter
public class Position {
    private static final Logger log = LoggerFactory.getLogger(Position.class);

    private long position;
    private double realizedPnl;
    private double unrealizedPnl;
    private double totalPnl;
    private double avgPrice;
    private double tradedQty;

    public void updatePnl(MarketOrderBook.BBO bbo) {
        if (position == 0 || bbo.getBidPrice() == PRICE_INVALID || bbo.getAskPrice() == PRICE_INVALID) {
            return;
        }

        double midPrice = (bbo.getBidPrice() + bbo.getAskPrice()) * 0.5;
        if (position > 0) {
            unrealizedPnl = (midPrice - avgPrice) * position;
        } else {
            unrealizedPnl = (avgPrice - midPrice) * position * (-1);
        }

        totalPnl = realizedPnl + unrealizedPnl;
    }

    public void addFill(OrderMessage orderMessage) {
        double fillPrice = orderMessage.getPrice();
        long fillQty = orderMessage.getExecQty();
        double fillValue = fillPrice * fillQty;
        Side side = orderMessage.getSide();
        int sign = side == Side.BUY ? 1 : -1;

        if (position == 0) {
            avgPrice = fillPrice;
        } else if (position * sign > 0) {
            // increase position
            avgPrice = (avgPrice * position + fillValue) / (position + fillQty);
        } else {
            // decrease position
            long closedQty = Math.min(Math.abs(position), fillQty);
            if (fillQty > closedQty) {
                // open position
                avgPrice = fillPrice;
            }
            realizedPnl += sign * (avgPrice - fillPrice) * closedQty;
        }

        position += sign * fillQty;
        tradedQty += fillQty;
        log.info("addFill {}{}@{} {}", side == Side.BUY ? '+' : '-', fillQty, fillPrice, this);
    }

    @Override
    public String toString() {
        return "Position{" +
                "avgPrice=" + avgPrice +
                ", position=" + position +
                ", realizedPnl=" + realizedPnl +
                ", totalPnl=" + totalPnl +
                ", tradedQty=" + tradedQty +
                ", unrealizedPnl=" + unrealizedPnl +
                '}';
    }
}
