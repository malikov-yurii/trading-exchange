package com.exchange.matching;


public class OrdersAtPriceMap {

    private final OrdersAtPrice[] ordersAtPrice;

    public OrdersAtPriceMap(int maxPriceLevels) {
        ordersAtPrice = new OrdersAtPrice[maxPriceLevels];
    }

    public OrdersAtPrice get(long price) {
        if (price < 0 || price >= ordersAtPrice.length) {
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