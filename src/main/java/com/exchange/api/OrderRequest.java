package com.exchange.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderRequest {

    private long seqNum;
    private OrderRequestType type = OrderRequestType.INVALID;
    private long clientId;
    private long tickerId;
    private long orderId;
    private Side side = Side.INVALID;
    private long price;
    private long qty;

    public OrderRequest(OrderRequestType type, long clientId, long tickerId, long orderId, Side side, long price,
                        long qty) {
        this.type = type;
        this.clientId = clientId;
        this.tickerId = tickerId;
        this.orderId = orderId;
        this.side = side;
        this.price = price;
        this.qty = qty;
    }

    @Override
    public String toString() {
        return String.format(
                "ClientRequest{ %2d: %-10s clOrdId=%-4d %-5s %7s client=%d ticker=%d }",
                seqNum,
                type,
                orderId,
                side,
                qty + "@" + price,
                clientId,
                tickerId
        );
    }

}