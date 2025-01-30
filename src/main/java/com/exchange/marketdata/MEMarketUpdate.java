package com.exchange.marketdata;
import com.exchange.orderserver.Side;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MEMarketUpdate {

    private MarketUpdateType type = MarketUpdateType.INVALID;
    private long orderId;
    private long tickerId;
    private Side side = Side.INVALID;
    private long price;
    private long qty;
    private long priority;

    @Override
    public String toString() {
        return "MEMarketUpdate ["
                + "type:" + type
                + " ticker:" + tickerId
                + " oid:" + orderId
                + " side:" + side
                + " qty:" + qty
                + " price:" + price
                + " priority:" + priority
                + "]";
    }

}