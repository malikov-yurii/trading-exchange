package trading.api;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MarketUpdate {

    private long seqNum;
    private MarketUpdateType type = MarketUpdateType.INVALID;
    private long orderId;
    private long tickerId;
    private Side side = Side.INVALID;
    private long price;
    private long qty;
    private long priority;

    public MarketUpdate(MarketUpdateType marketUpdateType, long marketOrderId, long tickerId, Side side, long price, long qty, long priority, long seqNum) {
        set(marketUpdateType, marketOrderId, tickerId, side, price, qty, priority, seqNum);
    }

    public void set(MarketUpdateType marketUpdateType, long marketOrderId, long tickerId, Side side, long price, long qty, long priority, long seqNum) {
        this.type = marketUpdateType;
        this.orderId = marketOrderId;
        this.tickerId = tickerId;
        this.side = side;
        this.price = price;
        this.qty = qty;
        this.priority = priority;
        this.seqNum = seqNum;
    }

    public void reset() {
        seqNum = 0L;
        type = MarketUpdateType.INVALID;
        orderId = 0L;
        tickerId = 0L;
        side = Side.INVALID;
        price = 0L;
        qty = 0L;
        priority = 0L;
    }

    public static void copy(MarketUpdate from, MarketUpdate to) {
        to.setSeqNum(from.getSeqNum());
        to.setType(from.getType());
        to.setOrderId(from.getOrderId());
        to.setTickerId(from.getTickerId());
        to.setSide(from.getSide());
        to.setPrice(from.getPrice());
        to.setQty(from.getQty());
        to.setPriority(from.getPriority());
    }

    @Override
    public String toString() {
        return String.format(
                "MarketUpdate{ %2d: %-7s orderId=%-3d %-4s %7s ticker=%d priority=%d }",
                seqNum,
                type,
                orderId,
                side,
                qty + "@" + price,
                tickerId,
                priority
        );
    }

}