package com.exchange.orderserver;

import lombok.Data;

@Data
public class OMClientResponse {

    private long seqNum;
    private MEClientResponse meClientResponse = new MEClientResponse();

    @Override
    public String toString() {
        return "OMClientResponse [" + "seq:" + seqNum + " " + meClientResponse + "]";
    }

}