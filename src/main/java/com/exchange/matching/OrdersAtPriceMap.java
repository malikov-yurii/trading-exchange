package com.exchange.matching;


public class OrdersAtPriceMap {

    private final MEOrdersAtPrice[] ordersAtPrice;

    public OrdersAtPriceMap(int maxPriceLevels) {
        ordersAtPrice = new MEOrdersAtPrice[maxPriceLevels];
    }

    public MEOrdersAtPrice get(long price) {
        if (price < 0 || price >= ordersAtPrice.length) {
            return null;
        }
        return ordersAtPrice[(int) price];
    }

    public void put(long price, MEOrdersAtPrice update) {
        if (price >= 0 && price < ordersAtPrice.length) {
            ordersAtPrice[(int) price] = update;
        }
    }

    public void remove(long price) {
        if (price >= 0 && price < ordersAtPrice.length) {
            ordersAtPrice[(int) price] = null;
        }
    }
}