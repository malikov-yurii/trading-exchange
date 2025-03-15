package trading.api;

public enum Side {

    INVALID,
    BUY,
    SELL;

    public int getSign() {
        if (this == BUY) {
            return 1;
        } else if (this == SELL) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int getMaxSides() {
        return 2;
    }

    public int toIndex() {
        if (this == BUY) {
            return 0;
        } else if (this == SELL) {
            return 1;
        } else {
            return -1;
        }
    }

}