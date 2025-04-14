package trading.exchange.matching;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrdersAtPriceMap {
    private static final Logger log = LoggerFactory.getLogger(OrdersAtPriceMap.class);

    private final OrdersAtPrice[] ordersAtPrice;

    public OrdersAtPriceMap(int maxPriceLevels) {
        ordersAtPrice = new OrdersAtPrice[maxPriceLevels];
    }

    public OrdersAtPrice get(long price) {
        if (price < 0 || price >= ordersAtPrice.length) {
            log.error("Invalid price: {}.", price);
            return null;
        }
        return ordersAtPrice[(int) price];
    }

    public void put(OrdersAtPrice ordersAtPrice) {
        long price = ordersAtPrice.getPrice();
        if (price >= 0 && price < this.ordersAtPrice.length) {
            this.ordersAtPrice[(int) price] = ordersAtPrice;
        }
    }

    public void remove(long price) {
        if (price >= 0 && price < ordersAtPrice.length) {
            ordersAtPrice[(int) price] = null;
        }
    }
}