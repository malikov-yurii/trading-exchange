package trading.exchange.matching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.common.Constants;

public class OrderMap {
    private static final Logger log = LoggerFactory.getLogger(OrderMap.class);

    private final Order[] orders;

    public OrderMap(int maxOrderIds) {
        orders = new Order[maxOrderIds];
    }

    public Order get(long clOrdId) {
        if (clOrdId < 0 || clOrdId >= orders.length) {
            logInvalid(clOrdId);
            return null;
        }
        return orders[(int) clOrdId];
    }

    private static void logInvalid(long clOrdId) {
        log.error("Invalid clOrdId: {}. ME_MAX_ORDER_IDS: {}", clOrdId, Constants.ME_MAX_ORDER_IDS);
    }

    public void put(Order order) {
        long clOrdId = order.getClientOrderId();
        if (clOrdId >= 0 && clOrdId < orders.length) {
            orders[(int) clOrdId] = order;
        } else {
            logInvalid(clOrdId);
        }
    }

    public void remove(long clOrdId) {
        if (clOrdId >= 0 && clOrdId < orders.length) {
            orders[(int) clOrdId] = null;
        } else {
            logInvalid(clOrdId);
        }
    }

}