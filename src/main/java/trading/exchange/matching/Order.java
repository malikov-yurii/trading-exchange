package trading.exchange.matching;

import lombok.NoArgsConstructor;
import trading.api.Side;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A single order in the limit order book.
 */
@Data
@NoArgsConstructor
public class Order {

    private long tickerId;
    private long clientId;
    private long clientOrderId;
    private long marketOrderId;
    private Side side;
    private long price;
    private long qty;
    private long priority;
    private Order prevOrder;
    private Order nextOrder;

    public Order(long tickerId, long clientId, long clientOrderId, long marketOrderId, Side side, long price, long qty, long priority, Order prevOrder, Order nextOrder) {
        set(tickerId, clientId, clientOrderId, marketOrderId, side, price, qty, priority, prevOrder, nextOrder);
    }

    public void set(long tickerId, long clientId, long clientOrderId, long marketOrderId, Side side, long price, long qty, long priority, Order prevOrder, Order nextOrder) {
        this.tickerId = tickerId;
        this.clientId = clientId;
        this.clientOrderId = clientOrderId;
        this.marketOrderId = marketOrderId;
        this.side = side;
        this.price = price;
        this.qty = qty;
        this.priority = priority;
        this.prevOrder = prevOrder;
        this.nextOrder = nextOrder;
    }

    @Override
    public String toString() {
        return "MEOrder ["
                + "tickerId:" + tickerId
                + " clientId:" + clientId
                + " clientOrderId:" + clientOrderId
                + " marketOrderId:" + marketOrderId
                + " side:" + side
                + " price:" + price
                + " qty:" + qty
                + " priority:" + priority
                + "]";
    }

}