package com.exchange.orderserver;

import lombok.Getter;

@Getter
public enum ClientResponseType {

    INVALID((byte) 0),
    ACCEPTED((byte) 1),
    CANCELED((byte) 2),
    FILLED((byte) 3),
    CANCEL_REJECTED((byte) 4);

    private final byte value;

    ClientResponseType(byte value) {
        this.value = value;
    }

}