package trading.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderResponse {

    private long seqNum;
    private OrderResponseType type = OrderResponseType.INVALID;
    private long clientId;
    private long tickerId;
    private long clientOrderId;
    private long marketOrderId;
    private Side side = Side.INVALID;
    private long price;
    private long execQty;
    private long leavesQty;

    public OrderResponse(OrderResponseType type, long clientId, long tickerId, long clientOrderId,
                         long marketOrderId, Side side, long price, long execQty, long leavesQty) {
        set(type, clientId, tickerId, clientOrderId,
                marketOrderId, side, price, execQty, leavesQty);
    }

    public void set(OrderResponseType type, long clientId, long tickerId, long clientOrderId,
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

    public void reset() {
        seqNum = 0L;
        type = OrderResponseType.INVALID;
        clientId = 0L;
        tickerId = 0L;
        clientOrderId = 0L;
        marketOrderId = 0L;
        side = Side.INVALID;
        price = 0L;
        execQty = 0L;
        leavesQty = 0L;
    }

    public static void copy(OrderResponse from, OrderResponse to) {
        to.setSeqNum(from.getSeqNum());
        to.setType(from.getType());
        to.setClientId(from.getClientId());
        to.setTickerId(from.getTickerId());
        to.setClientOrderId(from.getClientOrderId());
        to.setMarketOrderId(from.getMarketOrderId());
        to.setSide(from.getSide());
        to.setPrice(from.getPrice());
        to.setExecQty(from.getExecQty());
        to.setLeavesQty(from.getLeavesQty());
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