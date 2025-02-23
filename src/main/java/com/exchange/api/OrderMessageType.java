package com.exchange.api;

import lombok.Getter;

@Getter
public enum OrderMessageType {

    INVALID((byte) 0),
    ACCEPTED((byte) 1),
    CANCELED((byte) 2),
    FILLED((byte) 3),
    CANCEL_REJECTED((byte) 4);

    private final byte value;

    OrderMessageType(byte value) {
        this.value = value;
    }

}