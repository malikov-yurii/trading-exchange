package com.exchange.orderserver;

import lombok.Data;

/**
 * Client request used internally by the matching engine.
 */
@Data
public class MEClientRequest {

    private ClientRequestType type = ClientRequestType.INVALID;
    private long clientId;
    private long tickerId;
    private long orderId;
    private Side side = Side.INVALID;
    private long price;
    private long qty;

    @Override
    public String toString() {
        return "MEClientRequest ["
                + "type:" + type
                + " client:" + clientId
                + " ticker:" + tickerId
                + " oid:" + orderId
                + " side:" + side
                + " qty:" + qty
                + " price:" + price
                + "]";
    }

}