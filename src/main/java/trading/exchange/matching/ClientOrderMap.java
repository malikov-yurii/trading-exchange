package trading.exchange.matching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.common.Constants;

public class ClientOrderMap {
    private static final Logger log = LoggerFactory.getLogger(ClientOrderMap.class);

    private final OrderMap[] clientOrderMaps;

    public ClientOrderMap(int maxNumClients, int maxOrderIds) {
        clientOrderMaps = new OrderMap[maxNumClients];
        for (int i = 0; i < maxNumClients; i++) {
            clientOrderMaps[i] = new OrderMap(maxOrderIds);
        }
    }

    public Order get(long clientId, long clientOrderId) {
        if (clientId < 0 || clientId >= Constants.ME_MAX_NUM_CLIENTS) {
            log.error("Invalid clientId: {}. ME_MAX_NUM_CLIENTS: {}", clientId, Constants.ME_MAX_NUM_CLIENTS);
            return null;
        }
        OrderMap orderMap = get(clientId);
        return orderMap != null ? orderMap.get(clientOrderId) : null;
    }

    public void put(Order order) {
        get(order.getClientId()).put(order);
    }

    public void remove(Order order) {
        get(order.getClientId()).remove(order.getClientOrderId());
    }

    private OrderMap get(long clientId) {
        if (clientId < 0 || clientId >= clientOrderMaps.length) {
            log.error("Invalid clientId: {}. ME_MAX_NUM_CLIENTS: {}", clientId, Constants.ME_MAX_NUM_CLIENTS);
            return null;
        }
        return clientOrderMaps[(int) clientId];
    }

}
