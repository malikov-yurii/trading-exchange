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

}