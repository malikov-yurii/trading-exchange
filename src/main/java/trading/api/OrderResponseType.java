package trading.api;

import lombok.Getter;

@Getter
public enum OrderResponseType {

    INVALID((byte) 0),
    ACCEPTED((byte) 1),
    CANCELED((byte) 2),
    FILLED((byte) 3),
    CANCEL_REJECTED((byte) 4),
    REQUEST_REJECT((byte) 5);

    private final byte value;

    OrderResponseType(byte value) {
        this.value = value;
    }

}