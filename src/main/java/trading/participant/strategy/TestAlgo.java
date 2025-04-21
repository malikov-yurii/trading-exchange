package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.OrderMessageType;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;
import trading.common.Utils;
import trading.participant.ordergateway.OrderGatewayClient;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static trading.api.OrderMessageType.ACCEPTED;
import static trading.api.OrderMessageType.CANCELED;
import static trading.api.OrderMessageType.CANCEL_REJECTED;
import static trading.api.OrderMessageType.REQUEST_REJECT;
import static trading.common.Utils.env;
import static trading.common.Utils.getTestTag;

public class TestAlgo implements TradingAlgo {

    record TestRequest(OrderRequest request, LocalDateTime sent) {}

    private static final Logger log = LoggerFactory.getLogger(TestAlgo.class);
    public final int WARMUP_ORDER_NUMBER;

    private final TradeEngine tradeEngine;

    private volatile boolean isRunning;
    private final AtomicLong nextOrderId = new AtomicLong(0);
    private final Random random = new Random();
    private int clientNum;
    private int tickerNum;
    private int sleepTime;
    private final OrderRequest newOrderRequest = new OrderRequest();
    private final OrderRequest cancelOrderRequest = new OrderRequest();
    private final AtomicReference<TestRequest> lastRequest = new AtomicReference<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "TestAlgo-Cron"));

    private int orderNum;
    private volatile int currentTestId;

    public TestAlgo(TradeEngine tradeEngine) {
        log.info("TradingAlgo. Init.");
        this.tradeEngine = tradeEngine;
        this.WARMUP_ORDER_NUMBER = Integer.valueOf(env("WARMUP_ORDER_NUMBER", "400000"));
        sleepTime = Integer.parseInt(env("SLEEP_TIME_MS", "500"));
        orderNum = 500 + Integer.parseInt(env("TOTAL_ORDER_NUMBER", "1_0"));
        clientNum = 4;
        tickerNum = 2;
        log.info("TOTAL_ORDER_NUMBER: {}", orderNum);
    }

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
        if (currentTestId == 5) {
            synchronized (newOrderRequest) {
                long clientOrderId = orderMessage.getClientOrderId();
//                log.info("{} Trace. onOrderUpdate. {}, newOrderRequest.getOrderId():{}, cancelOrderRequest.getOrderId():{}", getTestTag(clientOrderId), orderMessage, newOrderRequest.getOrderId(), cancelOrderRequest.getOrderId());

                OrderMessageType type = orderMessage.getType();
                if (clientOrderId == newOrderRequest.getOrderId() && type == ACCEPTED) {
                    log.info("{} onOrderUpdate. Received New Order Ack {}: {}", getTestTag(clientOrderId), type, orderMessage);
                    sendCancelOrder();
                } else if (clientOrderId == newOrderRequest.getOrderId() && type == REQUEST_REJECT) {
                    /* Indicates New Order was Submitted successfully earlier. Thus, dup new order with the same clOrdId is rejected  */
                    log.info("{} onOrderUpdate. Received New Order Nack {}: {}", getTestTag(clientOrderId), type, orderMessage);
                    sendCancelOrder();
                } else if (clientOrderId == cancelOrderRequest.getOrderId() && type == CANCELED) {
                    log.info("{} onOrderUpdate. Received Cancel Order Ack {}: {}", getTestTag(clientOrderId), type, orderMessage);
                    sendNewOrder();
                } else if (clientOrderId == cancelOrderRequest.getOrderId() && type == CANCEL_REJECTED) {
                    /* Indicates order was canceled earlier, and it is not live anymore */
                    log.info("{} onOrderUpdate. Received Cancel Nack {}: {}", getTestTag(clientOrderId), type, orderMessage);
                    sendNewOrder();
                } else {
                    log.error("onOrderUpdate. Unexpected {}", orderMessage);
                }
            }
        }
    }

    private void sendCancelOrder() {
        OrderRequest.copy(newOrderRequest, cancelOrderRequest);
        cancelOrderRequest.setType(OrderRequestType.CANCEL);
        tradeEngine.sendOrderRequest(cancelOrderRequest);
        lastRequest.set(new TestRequest(cancelOrderRequest, LocalDateTime.now()));
    }

    private void test5() {
        try {
            test5_scheduleResendCheck();
            Thread.sleep(10_000);
            synchronized (newOrderRequest) {
                // Initial New Order Request to kick off looping: all later requests are generated on exchange responses
                sendNewOrder();
            }
        } catch (Exception e) {
            log.error("Error in TestAlgo", e);
        }
    }

    private void test5_scheduleResendCheck() {
        int timeoutMs = 300;
        int repeatIntervalMs = 500;
        scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        synchronized (newOrderRequest) {
                            TestRequest testRequest = lastRequest.get();
                            if (testRequest == null) {
                                return;
                            }
                            OrderRequest last = testRequest.request;
                            if (last == null) {
                                log.info("Last == NULL");
                                return;
                            }
                            log.info("{} CheckAge: {}", Utils.getTestTag(last.getOrderId()), testRequest);
                            long age = Duration.between(testRequest.sent, LocalDateTime.now()).toMillis();
                            if (age > timeoutMs) {
                                if (last == newOrderRequest) {
                                    log.info("{} CheckAge. Resend. Age: {}ms. NewOrderRequest {}", Utils.getTestTag(last.getOrderId()), age, last);
                                    tradeEngine.sendOrderRequest(last);
                                } else if (last == cancelOrderRequest) {
                                    log.info("{} CheckAge. Resend. Age: {}ms. CancelOrderRequest {}", Utils.getTestTag(last.getOrderId()), age, last);
                                    tradeEngine.sendOrderRequest(last);
                                } else {
                                    log.error("{} CheckAge. Resend. Failed.", Utils.getTestTag(last.getOrderId()));
                                }
                            }
                        }
                    } catch (Exception exception) {
                        log.error("CheckAge failed", exception);
                    }
                },
                5000, // initial delay
                repeatIntervalMs, // repeat interval
                TimeUnit.MILLISECONDS
        );
    }

    private void test4() {
        try {
            currentTestId = 4;

            while (isRunning && nextOrderId.get() < orderNum) {
                OrderRequest sellNewOrderRequest = new OrderRequest();
                sendNewOrderRequest(sellNewOrderRequest, 0, 0, Side.SELL, 100, 10);

                OrderRequest buyNewOrderRequest = new OrderRequest();
                sendNewOrderRequest(buyNewOrderRequest, 1, 0, Side.BUY, 30, 10);

                OrderRequest cancelSellNewOrderRequest = new OrderRequest(sellNewOrderRequest);
                cancelSellNewOrderRequest.setType(OrderRequestType.CANCEL);
                tradeEngine.sendOrderRequest(cancelSellNewOrderRequest);
            }

        } catch (Exception e) {
            log.error("Error in TestAlgo", e);
        }
    }

    @Override
    public void init() {
        isRunning = true;
        new Thread(() -> {
            try {
                currentTestId = Integer.parseInt(env("TEST_ID", null));
                log.info("TEST_ID: {}", currentTestId);
                switch (currentTestId) {
                    case 4 -> test4();
                    case 5 -> test5();
                    default -> throw new RuntimeException("test not supported :" + currentTestId);
                }
                log.info("-------------------------------> TEST{} DONE <------------------------------ last order id {}",
                        currentTestId, nextOrderId.get() - 1);
            } catch (Exception ex) {
                log.error("init. Failed. TEST_ID: " + currentTestId, ex);
            }
        }).start();
    }

    private void sendNewOrder() {
        sendNewOrderRequest(newOrderRequest, 0, 0, Side.SELL, 100, 10);
        lastRequest.set(new TestRequest(newOrderRequest, LocalDateTime.now()));
    }

    private void sendNewOrderRequest(OrderRequest newOrderRequest, int clientId, int tickerId, Side side, int qty, int price) {
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

}
