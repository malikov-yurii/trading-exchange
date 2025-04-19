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

import static trading.common.Utils.env;
import static trading.common.Utils.getTestTag;

public class RandomOrderSender implements TradingAlgo {
    record TestRequest(OrderRequest request, LocalDateTime sent) {
    }

    private static final Logger log = LoggerFactory.getLogger(RandomOrderSender.class);
    public final int SLEEP_ORDER_ID;

    private final TradeEngine tradeEngine;
//    private final OrderGatewayClient orderGatewayClient;

    private volatile boolean isRunning;
    private final AtomicLong nextOrderId = new AtomicLong(1);
    private final Random random = new Random();
    private int clientNum;
    private int tickerNum;
    private int sleepTime;
    private final OrderRequest newOrderRequest = new OrderRequest();
    private final OrderRequest cancelOrderRequest = new OrderRequest();
    private final AtomicReference<TestRequest> lastRequest = new AtomicReference<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "RandomOrderSender-Cron"));


    private int orderNum;
    private volatile int currentTestId;

    public RandomOrderSender(TradeEngine tradeEngine, OrderGatewayClient orderGatewayClient) {
        log.info("TradingAlgo. Init.");
        this.tradeEngine = tradeEngine;
        this.SLEEP_ORDER_ID = Integer.valueOf(env("SLEEP_ORDER_ID", "400000"));
        sleepTime = Integer.parseInt(env("SLEEP_TIME_MS", "500"));
        orderNum = Integer.parseInt(env("ORDER_NUM", "1_0"));
        clientNum = 4;
        tickerNum = 2;
        log.info("ORDER_NUM: {}", orderNum);
//        this.orderGatewayClient = orderGatewayClient;
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
                OrderMessageType type = orderMessage.getType();
                if (orderMessage.getClientOrderId() == newOrderRequest.getOrderId()
                        && type == OrderMessageType.ACCEPTED) {
                    // Step 5.2: On Ack New: Send Cancel
                    log.info("{} onOrderUpdate. Received Ack New: {}", getTestTag(orderMessage.getClientOrderId()), orderMessage);
                    sendCancelOrder();
                } else if (
                        orderMessage.getClientOrderId() == cancelOrderRequest.getOrderId()
                                && (type == OrderMessageType.CANCELED || type == OrderMessageType.CANCEL_REJECTED)
                ||
                        orderMessage.getClientOrderId() == newOrderRequest.getOrderId()
                                && type == OrderMessageType.REJECTED
                ) {

                    // Step 5.2: On Ack Cancel: Send New (loops to 5.3)
                    log.info("{} onOrderUpdate. Received {}: {}", getTestTag(orderMessage.getClientOrderId()), type, orderMessage);
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
            currentTestId = 5;
            int timeoutMs = 300;
            int repeatIntervalMs = 2000;
            scheduler.scheduleAtFixedRate(
                    () -> {
                        try {
                            OrderRequest last = lastRequest.get().request;
                            if (last == null) {
                                log.info("Last == NULL");
                                return;
                            }
                            log.info("{} CheckAge. lastRequest.get(): {}", Utils.getTestTag(last.getOrderId()), lastRequest.get());
                            long age = Duration.between(lastRequest.get().sent, LocalDateTime.now()).toMillis();
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
                        } catch (Exception exception) {
                            log.error("CheckAge failed", exception);
                        }
                    },
                    5000, // initial delay
                    repeatIntervalMs, // repeat interval
                    TimeUnit.MILLISECONDS
            );

            synchronized (newOrderRequest) {
                // Step 5.1: New Order
                sendNewOrder();
            }
        } catch (Exception e) {
            log.error("Error in RandomOrderSender", e);
        }
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
            log.error("Error in RandomOrderSender", e);
        }
    }

    @Override
    public void init() {
        isRunning = true;
        new Thread(() -> {
//            test4();
            test5();
            log.info("-------------------------------> TEST{} DONE <------------------------------ last order id {}",
                    currentTestId, nextOrderId.get() - 1);
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

        if (nextOrderId.get() == SLEEP_ORDER_ID) {
            log.info("Sleeping {}ms on next order |11={}|", sleepTime, SLEEP_ORDER_ID);
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
