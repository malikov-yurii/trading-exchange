package com.exchange.matching;

import com.exchange.orderserver.Side;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A single order in the limit order book.
 */
@Data
@AllArgsConstructor
public class MEOrder {

    private long tickerId;
    private long clientId;
    private long clientOrderId;
    private long marketOrderId;
    private Side side;
    private long price;
    private long qty;
    private long priority;
    private MEOrder prevOrder;
    private MEOrder nextOrder;

    @Override
    public String toString() {
        return "MEOrder ["
                + "tickerId:" + tickerId
                + " clientId:" + clientId
                + " clientOrderId:" + clientOrderId
                + " marketOrderId:" + marketOrderId
                + " side:" + side
                + " price:" + price
                + " qty:" + qty
                + " priority:" + priority
                + "]";
    }

}