package com.exchange;

public final class MEConstants {

    private MEConstants() {
    }

    public static final int LOG_QUEUE_SIZE = 8 * 1024 * 1024;
    public static final int ME_MAX_TICKERS = 8;
    public static final int ME_MAX_CLIENT_UPDATES = 256 * 1024;
    public static final int ME_MAX_MARKET_UPDATES = 256 * 1024;
    public static final int ME_MAX_NUM_CLIENTS = 20;
    public static final int ME_MAX_ORDER_IDS = 1024 * 1024;
    public static final int ME_MAX_PRICE_LEVELS = 256;

    public static final long ORDER_ID_INVALID = -1;
    public static final long PRICE_INVALID = -1;
    public static final long QTY_INVALID = -1;
    public static final long PRIORITY_INVALID = -1;

}