package com.exchange.orderserver;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClientRequest {

    private long seqNum;
    private ClientRequestType type = ClientRequestType.INVALID;
    private long clientId;
    private long tickerId;
    private long orderId;
    private Side side = Side.INVALID;
    private long price;
    private long qty;

    public ClientRequest(ClientRequestType type, long clientId, long tickerId, long orderId, Side side, long price,
                         long qty) {
        this.type = type;
        this.clientId = clientId;
        this.tickerId = tickerId;
        this.orderId = orderId;
        this.side = side;
        this.price = price;
        this.qty = qty;
    }

}