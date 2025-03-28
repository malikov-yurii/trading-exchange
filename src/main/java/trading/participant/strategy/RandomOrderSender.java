package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RandomOrderSender implements TradingAlgo {
    private static final Logger log = LoggerFactory.getLogger(RandomOrderSender.class);

    private final TradeEngine tradeEngine;

    private volatile boolean isRunning;
    private final AtomicLong nextOrderId = new AtomicLong();
    private final Random random = new Random();
    ;

    public RandomOrderSender(TradeEngine tradeEngine) {
        log.info("TradingAlgo. Init.");
        this.tradeEngine = tradeEngine;
    }

    @Override
    public void onOrderBookUpdate(long tickerId, long price, Side side, MarketOrderBook marketOrderBook) {
        log.info("onOrderBookUpdate. tickerId: {}, price: {}, side: {}", tickerId, price, side);
    }

    @Override
    public void onTradeUpdate(MarketUpdate marketUpdate, MarketOrderBook marketOrderBook) {
        log.info("onTradeUpdate. marketUpdate: {}", marketUpdate);
    }

    @Override
    public void onOrderUpdate(OrderMessage orderMessage) {
        log.info("onOrderUpdate. orderMessage: {}", orderMessage);
    }

    @Override
    public void init() {
        isRunning = true;
        new Thread(() -> {
            while (isRunning) {
                sleep(1_000);

                int qty = 50 + random.nextInt(10);
                int price = 8 + random.nextInt(12);

                sendRequest(Side.SELL, qty, price);

                sleep(1_000);
                sendRequest(Side.BUY, qty, price - 6 );
            }
        }).start();
    }

    private void sendRequest(Side side, int qty, int price) {
        OrderRequest orderRequest = new OrderRequest();
        orderRequest.setType(OrderRequestType.NEW);
        orderRequest.setClientId(10 + random.nextInt(10));
        orderRequest.setTickerId(0);

        orderRequest.setOrderId(nextOrderId.getAndIncrement());

        orderRequest.setSide(side);
        orderRequest.setPrice(price);
        orderRequest.setQty(qty);

        tradeEngine.sendOrderRequest(orderRequest);
    }

    public void shutdown() {
        isRunning = false;
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
