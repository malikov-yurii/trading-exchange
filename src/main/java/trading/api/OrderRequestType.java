package trading.api;

import lombok.Getter;

/**
 * Type of the order request sent by the trading client to the exchange.
 */
@Getter
public enum OrderRequestType {

    INVALID((byte) 0),
    NEW((byte) 1),
    CANCEL((byte) 2);

    private final byte value;

    OrderRequestType(byte value) {
        this.value = value;
    }

    public static OrderRequestType fromValue(byte value) {
        for (OrderRequestType type : OrderRequestType.values()) {
            if (type.value == value) {
                return type;
            }
        }
        return INVALID;
    }

}