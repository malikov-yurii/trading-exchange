package com.exchange.marketdata;
import com.exchange.orderserver.Side;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketUpdate {

    private long seqNum;
    private MarketUpdateType type = MarketUpdateType.INVALID;
    private long orderId;
    private long tickerId;
    private Side side = Side.INVALID;
    private long price;
    private long qty;
    private long priority;

    public MarketUpdate(MarketUpdateType marketUpdateType, long marketOrderId, long tickerId, Side side, long price,
                        long qty, long priority) {
        this.type = marketUpdateType;
        this.orderId = marketOrderId;
        this.tickerId = tickerId;
        this.side = side;
        this.price = price;
        this.qty = qty;
        this.priority = priority;
    }
}