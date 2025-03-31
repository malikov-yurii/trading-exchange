package trading.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderMessage {

    private long seqNum;
    private OrderMessageType type = OrderMessageType.INVALID;
    private long clientId;
    private long tickerId;
    private long clientOrderId;
    private long marketOrderId;
    private Side side = Side.INVALID;
    private long price;
    private long execQty;
    private long leavesQty;

    public OrderMessage(OrderMessageType type, long clientId, long tickerId, long clientOrderId,
                        long marketOrderId, Side side, long price, long execQty, long leavesQty) {
        this.type = type;
        this.clientId = clientId;
        this.tickerId = tickerId;
        this.clientOrderId = clientOrderId;
        this.marketOrderId = marketOrderId;
        this.side = side;
        this.price = price;
        this.execQty = execQty;
        this.leavesQty = leavesQty;
    }

    @Override
    public String toString() {
        return String.format(
                "OrderMessage{%2d: %-8s clOrdId=%-3d %-4s %7s leaves=%-3d mktOrdId=%d client=%d ticker=%d}",
                seqNum,
                type,
                clientOrderId,
                side,
                execQty + "@" + price,
                leavesQty,
                marketOrderId,
                clientId,
                tickerId
        );
    }


}