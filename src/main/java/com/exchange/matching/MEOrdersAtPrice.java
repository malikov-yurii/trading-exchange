package com.exchange.matching;

import com.exchange.orderserver.Side;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A price level in the limit order book.
 */
@Data
@AllArgsConstructor
public class MEOrdersAtPrice {
    private Side side;
    private long price;
    private MEOrder firstMeOrder;
    private MEOrdersAtPrice prevEntry;
    private MEOrdersAtPrice nextEntry;

    @Override
    public String toString() {
        return "MEOrdersAtPrice["
                + "side:" + side
                + " price:" + price
                + " firstMeOrder:" + firstMeOrder
                + " prev:" + (prevEntry != null ? prevEntry.getPrice() : null)
                + " next:" + (nextEntry != null ? nextEntry.getPrice() : null)
                + "]";
    }

}