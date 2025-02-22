package com.participant;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExchangeResponse {
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

//    @Override
//    public String toString() {
//        return "ExchangeResponse{" +
//                "seq=" + seqNum +
//                " " + type +
//                " clOrdId=" + clientOrderId +
//                " " + side +
//                " " + execQty + "@" + price +
//                " leaves=" + leavesQty +
//                " mktOrdID=" + marketOrderId +
//                " client=" + clientId +
//                " ticker=" + tickerId +
//                '}';
//    }

    @Override
    public String toString() {
        // We use %-N<type> to left-align each field in an N-character column.
        // Adjust the widths as necessary for your data.
        return String.format(
                "ExchangeResponse{ %2d: %-10s clOrdId=%-4d %-5s %7s leaves=%-5d mktOrdId=%d client=%d ticker=%d }",
                seqNum,
                type,
                clientOrderId,
                side,
                execQty + "@" + price,
                leavesQty,
                marketOrderId,
                clientId,
                tickerId
        );
    }
}

