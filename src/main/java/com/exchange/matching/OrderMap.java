package com.exchange.matching;

public class OrderMap {

    private final MEOrder[] orders;

    public OrderMap(int maxOrderIds) {
        orders = new MEOrder[maxOrderIds];
    }

    public MEOrder get(long orderId) {
        if (orderId < 0 || orderId >= orders.length) {
            return null;
        }
        return orders[(int) orderId];
    }

    public void put(long orderId, MEOrder order) {
        if (orderId >= 0 && orderId < orders.length) {
            orders[(int) orderId] = order;
        }
    }

    public void remove(long orderId) {
        if (orderId >= 0 && orderId < orders.length) {
            orders[(int) orderId] = null;
        }
    }

}