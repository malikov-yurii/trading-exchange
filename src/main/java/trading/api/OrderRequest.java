package trading.api;

import lombok.Data;
import lombok.NoArgsConstructor;
import trading.common.Constants;

@Data
@NoArgsConstructor
public class OrderRequest {

    private long seqNum = Constants.SEQ_NUM_INVALID;
    private OrderRequestType type = OrderRequestType.INVALID;
    private long clientId;
    private long tickerId;
    private long orderId;
    private Side side = Side.INVALID;
    private long price;
    private long qty;

    public OrderRequest(long seqNum, OrderRequestType type, long clientId, long tickerId, long orderId, Side side, long price, long qty) {
        set(seqNum, type, clientId, tickerId, orderId, side, price, qty);
    }

    public void set(long seqNum, OrderRequestType type, long clientId, long tickerId, long orderId, Side side, long price, long qty) {
        this.seqNum = seqNum;
        this.type = type;
        this.clientId = clientId;
        this.tickerId = tickerId;
        this.orderId = orderId;
        this.side = side;
        this.price = price;
        this.qty = qty;
    }

    public OrderRequest(OrderRequest other) {
        this.seqNum = other.seqNum;
        this.type = other.type;
        this.clientId = other.clientId;
        this.tickerId = other.tickerId;
        this.orderId = other.orderId;
        this.side = other.side;
        this.price = other.price;
        this.qty = other.qty;
    }

    public void reset() {
        this.seqNum = 0L;
        this.type = OrderRequestType.INVALID;
        this.clientId = 0L;
        this.tickerId = 0L;
        this.orderId = 0L;
        this.side = Side.INVALID;
        this.price = 0L;
        this.qty = 0L;
    }

    public static void copy(OrderRequest from, OrderRequest to) {
        to.setSeqNum(from.getSeqNum());
        to.setType(from.getType());
        to.setClientId(from.getClientId());
        to.setTickerId(from.getTickerId());
        to.setOrderId(from.getOrderId());
        to.setSide(from.getSide());
        to.setPrice(from.getPrice());
        to.setQty(from.getQty());
    }

    @Override
    public String toString() {
        return String.format(
                "ClientRequest{ seq: %2d, %-10s clOrdId=%-4d %-5s %7s client=%d ticker=%d }",
                seqNum,
                type,
                orderId,
                side,
                qty + "@" + price,
                clientId,
                tickerId
        );
    }

}