package com.exchange.orderserver;

import lombok.Getter;

/**
 * Type of the order request sent by the trading client to the exchange.
 */
@Getter
public enum ClientRequestType {

    INVALID((byte) 0),
    NEW((byte) 1),
    CANCEL((byte) 2);

    private final byte value;

    ClientRequestType(byte value) {
        this.value = value;
    }

}