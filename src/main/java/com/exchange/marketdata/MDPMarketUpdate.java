package com.exchange.marketdata;

import lombok.Data;

@Data
public class MDPMarketUpdate {

    private long seqNum;
    private MEMarketUpdate meMarketUpdate = new MEMarketUpdate();

    @Override
    public String toString() {
        return "MDPMarketUpdate ["
                + "seq:" + seqNum
                + " " + meMarketUpdate
                + "]";
    }

}