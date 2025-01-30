package com.exchange.orderserver;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MEClientResponse {

    private ClientResponseType type = ClientResponseType.INVALID;
    private long clientId;
    private long tickerId;
    private long clientOrderId;
    private long marketOrderId;
    private Side side = Side.INVALID;
    private long price;
    private long execQty;
    private long leavesQty;

    @Override
    public String toString() {
        return "MEClientResponse ["
                + "type:" + type
                + " client:" + clientId
                + " ticker:" + tickerId
                + " coid:" + clientOrderId
                + " moid:" + marketOrderId
                + " side:" + side
                + " exec_qty:" + execQty
                + " leaves_qty:" + leavesQty
                + " price:" + price
                + "]";
    }

}