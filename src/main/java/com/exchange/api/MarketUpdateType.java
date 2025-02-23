package com.exchange.api;

import lombok.Getter;

@Getter
public enum MarketUpdateType {

    INVALID((byte) 0),
    CLEAR((byte) 1),
    ADD((byte) 2),
    MODIFY((byte) 3),
    CANCEL((byte) 4),
    TRADE((byte) 5),
    SNAPSHOT_START((byte) 6),
    SNAPSHOT_END((byte) 7);

    private final byte value;

    MarketUpdateType(byte value) {
        this.value = value;
    }

}
