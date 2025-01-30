package com.exchange.orderserver;

import lombok.Data;

/**
 * Client request structure published over the network by the order gateway client.
 */
@Data
public class OMClientRequest {

    private long seqNum;
    private MEClientRequest meClientRequest = new MEClientRequest();

    @Override
    public String toString() {
        return "OMClientRequest ["
                + "seq:" + seqNum
                + " " + meClientRequest
                + "]";
    }

}