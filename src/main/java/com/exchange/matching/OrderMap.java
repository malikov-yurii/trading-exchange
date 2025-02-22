package com.exchange.matching;

public class OrderMap {

    private final Order[] orders;

    public OrderMap(int maxOrderIds) {
        orders = new Order[maxOrderIds];
    }

    public Order get(long clientOrderId) {
        if (clientOrderId < 0 || clientOrderId >= orders.length) {
            return null;
        }
        return orders[(int) clientOrderId];
    }

    public void put(Order order) {
        long orderId = order.getClientOrderId();
        if (orderId >= 0 && orderId < orders.length) {
            orders[(int) orderId] = order;
        }
    }

    public void remove(long clientOrderId) {
        if (clientOrderId >= 0 && clientOrderId < orders.length) {
            orders[(int) clientOrderId] = null;
        }
    }

}