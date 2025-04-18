package trading.exchange.matching;

import lombok.NoArgsConstructor;
import trading.api.Side;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A price level in the limit order book.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrdersAtPrice {

    private Side side;
    private long price;
    private Order firstOrder;
    private OrdersAtPrice prev;
    private OrdersAtPrice next;

    @Override
    public String toString() {
        return "MEOrdersAtPrice["
                + "side:" + side
                + " price:" + price
                + " firstMeOrder:" + firstOrder
                + " prev:" + (prev != null ? prev.getPrice() : null)
                + " next:" + (next != null ? next.getPrice() : null)
                + "]";
    }

    public void set(Side side, long price, Order order, OrdersAtPrice prev, OrdersAtPrice next) {
        this.side = side;
        this.price = price;
        this.firstOrder = order;
        this.prev = prev;
        this.next = next;
    }

}