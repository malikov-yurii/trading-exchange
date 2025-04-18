package trading.exchange.matching;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.Side;
import trading.common.ObjectPool;

public class OrdersAtPriceMap {
    private static final Logger log = LoggerFactory.getLogger(OrdersAtPriceMap.class);

    private final OrdersAtPrice[] ordersAtPrice;
    // TODO Refactor and get rid of pool, just use the array
    private final ObjectPool<OrdersAtPrice> ordersAtPricePool = new ObjectPool<>(1000, OrdersAtPrice::new);

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
            if (ordersAtPrice[(int) price] != null) {
                ordersAtPricePool.release(ordersAtPrice[(int) price]);
                ordersAtPrice[(int) price] = null;
            }
        }
    }

    public OrdersAtPrice createNew(Side side, long price, Order order, OrdersAtPrice prev, OrdersAtPrice next) {
        OrdersAtPrice ordersAtPrice = ordersAtPricePool.acquire();
        ordersAtPrice.set(side, price, order, prev, next);
        put(ordersAtPrice);
        return ordersAtPrice;
    }

}