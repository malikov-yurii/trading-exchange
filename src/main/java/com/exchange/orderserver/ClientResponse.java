package com.exchange.orderserver;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClientResponse {

    private long seqNum;
    private ClientResponseType type = ClientResponseType.INVALID;
    private long clientId;
    private long tickerId;
    private long clientOrderId;
    private long marketOrderId;
    private Side side = Side.INVALID;
    private long price;
    private long execQty;
    private long leavesQty;

    public ClientResponse(ClientResponseType type, long clientId, long tickerId, long clientOrderId,
                          long marketOrderId, Side side, long price, long execQty, long leavesQty) {
        this.type = type;
        this.clientId = clientId;
        this.tickerId = tickerId;
        this.clientOrderId = clientOrderId;
        this.marketOrderId = marketOrderId;
        this.side = side;
        this.price = price;
        this.execQty = execQty;
        this.leavesQty = leavesQty;
    }

}