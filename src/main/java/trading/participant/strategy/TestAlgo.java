package trading.participant.strategy;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static trading.common.Utils.env;

@Getter
public abstract class TestAlgo implements TradingAlgo {

    private static final Logger log = LoggerFactory.getLogger(TestAlgo.class);
    public final int WARMUP_ORDER_NUMBER;

    private final TradeEngine tradeEngine;

    private volatile boolean isRunning;
    private final AtomicLong nextOrderId = new AtomicLong(0);
    private final Random random = new Random();
    private final int clientNum;
    private final int tickerNum;
    private final int sleepTime;
    @Getter
    private final TestRequest orderRequest = new TestRequest();
    @Getter
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "TestAlgo-Cron"));

    private int orderNum;
    private volatile int currentTestId;

    public TestAlgo(TradeEngine tradeEngine) {
        log.info("TradingAlgo. Init.");
        this.tradeEngine = tradeEngine;
        this.WARMUP_ORDER_NUMBER = Integer.valueOf(env("WARMUP_ORDER_NUMBER", "400000"));
        sleepTime = Integer.parseInt(env("SLEEP_TIME_MS", "500"));
        orderNum = Integer.parseInt(env("TOTAL_ORDER_NUMBER", "1_0"));
        clientNum = 4;
        tickerNum = 2;
        log.info("TOTAL_ORDER_NUMBER: {}", orderNum);
        orderNum += 500;
    }

    @Override
    public void init() {
        log.info("TradingAlgo. Init.");
        isRunning = true;
        new Thread(() -> {
            try {
                this.run();
                log.info("-------------------------------> TEST{} DONE <------------------------------ last order id {}",
                        currentTestId, nextOrderId.get() - 1);
            } catch (Exception ex) {
                log.error("init. Failed. TEST_ID: " + currentTestId, ex);
            }
        }).start();
    }

    public abstract void run();

    @Override
    public void onOrderBookUpdate(long tickerId, long price, Side side, MarketOrderBook marketOrderBook) {
//        log.info("onOrderBookUpdate. tickerId: {}, price: {}, side: {}", tickerId, price, side);
    }

    @Override
    public void onTradeUpdate(MarketUpdate marketUpdate, MarketOrderBook marketOrderBook) {
//        log.info("onTradeUpdate. marketUpdate: {}", marketUpdate);
    }

    @Override
    public void onOrderUpdate(OrderMessage orderMessage) {
    }

    protected void sendOrderRequest(OrderRequest orderRequest) {
        tradeEngine.sendOrderRequest(orderRequest);
    }

    protected void sendNewOrderRequest(OrderRequest newOrderRequest, int clientId, int tickerId, Side side, int qty, int price) {
        newOrderRequest.setType(OrderRequestType.NEW);
        newOrderRequest.setClientId(clientId);
        newOrderRequest.setTickerId(tickerId);
        newOrderRequest.setOrderId(nextOrderId.getAndIncrement());
        newOrderRequest.setSide(side);
        newOrderRequest.setQty(qty);
        newOrderRequest.setPrice(price);
        tradeEngine.sendOrderRequest(newOrderRequest);

        if (nextOrderId.get() == WARMUP_ORDER_NUMBER) {
            log.info("Sleeping {}ms on next order |11={}|", sleepTime, WARMUP_ORDER_NUMBER);
            sleep(sleepTime);
        }
    }

    private static void sleep(int millis) {
        if (millis < 1) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        isRunning = false;
    }

    static class TestRequest extends OrderRequest {
        LocalDateTime newOrderTime;
        LocalDateTime cancelOrderTime;
        LocalDateTime resendingTime;
        @Override
        public String toString() {
            return super.toString()
                    + ", newOrderTime:" + newOrderTime
                    + ", cancelOrderTime:" + cancelOrderTime
                    + ", resendingTime:" + resendingTime
                    ;
        }
    }

}
