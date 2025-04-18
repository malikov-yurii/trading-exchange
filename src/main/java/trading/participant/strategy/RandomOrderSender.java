package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;
import trading.participant.ordergateway.OrderGatewayClient;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static trading.common.Utils.env;

public class RandomOrderSender implements TradingAlgo {
    private static final Logger log = LoggerFactory.getLogger(RandomOrderSender.class);
    public final int SLEEP_ORDER_ID;

    private final TradeEngine tradeEngine;
//    private final OrderGatewayClient orderGatewayClient;

    private volatile boolean isRunning;
    private final AtomicLong nextOrderId = new AtomicLong();
    private final Random random = new Random();
    private int clientNum;
    private int tickerNum;
    private int sleepTime;

    public RandomOrderSender(TradeEngine tradeEngine, OrderGatewayClient orderGatewayClient) {
        log.info("TradingAlgo. Init.");
        this.tradeEngine = tradeEngine;
        this.SLEEP_ORDER_ID = Integer.valueOf(env("SLEEP_ORDER_ID", "400000"));
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
//        log.info("onOrderUpdate. orderMessage: {}", orderMessage);
    }

    @Override
    public void init() {
        isRunning = true;
        new Thread(() -> {
            try {
                clientNum = 4;
                tickerNum = 2;
                sleepTime = Integer.parseInt(env("SLEEP_TIME_MS", "500"));
                final int orderNum = Integer.parseInt(env("ORDER_NUM", "1_000"));
                log.info("ORDER_NUM: {}", orderNum);

                log.info("-------------------------------> TEST2 ");

                while (isRunning && nextOrderId.get() < orderNum) {
                    OrderRequest sellNewOrderRequest = new OrderRequest();
                    sendNewOrderRequest(sellNewOrderRequest, 0, 0, Side.SELL, 100, 10);

                    OrderRequest buyNewOrderRequest = new OrderRequest();
                    sendNewOrderRequest(buyNewOrderRequest, 1, 0, Side.BUY, 30, 10);

                    OrderRequest cancelSellNewOrderRequest = new OrderRequest(sellNewOrderRequest);
                    cancelSellNewOrderRequest.setType(OrderRequestType.CANCEL);
                    tradeEngine.sendOrderRequest(cancelSellNewOrderRequest);
                }
                log.info("-------------------------------> TEST2 DONE <------------------------------ last order id {}",
                        nextOrderId.get() - 1);

            } catch (Exception e) {
                log.error("Error in RandomOrderSender", e);
            }
        }).start();
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
