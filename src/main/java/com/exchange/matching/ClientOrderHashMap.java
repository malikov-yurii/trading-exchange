package com.exchange.matching;

import com.exchange.MEConstants;

public class ClientOrderHashMap {

    private final OrderMap[] clientOrderMaps;

    public ClientOrderHashMap(int maxNumClients, int maxOrderIds) {
        clientOrderMaps = new OrderMap[maxNumClients];
        for (int i = 0; i < maxNumClients; i++) {
            clientOrderMaps[i] = new OrderMap(maxOrderIds);
        }
    }

    public MEOrder get(long clientId, long orderId) {
        if (clientId >= 0 && clientId < MEConstants.ME_MAX_NUM_CLIENTS) {
            return null;
        }
        OrderMap orderMap = get(clientId);
        return orderMap != null ? orderMap.get(orderId) : null;
    }

    public void put(MEOrder order) {
        get(order.getClientId()).put(order.getClientOrderId(), order);
    }

    public void remove(MEOrder order) {
        get(order.getClientId()).remove(order.getClientOrderId());
    }

    private OrderMap get(long clientId) {
        if (clientId < 0 || clientId >= clientOrderMaps.length) {
            return null;
        }
        return clientOrderMaps[(int) clientId];
    }

}
