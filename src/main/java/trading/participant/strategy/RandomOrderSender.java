package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;
import trading.common.Utils;
import trading.participant.ordergateway.OrderGatewayClient;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RandomOrderSender implements TradingAlgo {
    private static final Logger log = LoggerFactory.getLogger(RandomOrderSender.class);

    private final TradeEngine tradeEngine;
    private final OrderGatewayClient orderGatewayClient;

    private volatile boolean isRunning;
    private final AtomicLong nextOrderId = new AtomicLong();
    private final Random random = new Random();
    private int clientNum;
    private int tickerNum;

    public RandomOrderSender(TradeEngine tradeEngine, OrderGatewayClient orderGatewayClient) {
        log.info("TradingAlgo. Init.");
        this.tradeEngine = tradeEngine;
        this.orderGatewayClient = orderGatewayClient;
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
            try {
                clientNum = 4;
                tickerNum = 2;
//            int clientNum = Constants.ME_MAX_NUM_CLIENTS;
//            int tickerNum = Constants.ME_MAX_TICKERS;

                OrderRequest[] newOrderRequests = new OrderRequest[clientNum * tickerNum];
                OrderRequest[] cancelOrderRequests = new OrderRequest[clientNum * tickerNum];
                for (int i = 0; i < newOrderRequests.length; i++) {
                    newOrderRequests[i] = new OrderRequest();
                    cancelOrderRequests[i] = new OrderRequest();
                }
                int sleepTime = Integer.parseInt(Utils.env("SLEEP_TIME_MS", "500"));

                while (isRunning) {
                    for (int clientId = 0; clientId < clientNum / 2; clientId++) {
                        for (int tickerId = 0; tickerId < tickerNum; tickerId++) {
                            OrderRequest orderRequest = getOrderRequest(newOrderRequests, clientId, tickerId);
                            sendNewOrderRequest(orderRequest, clientId, tickerId, Side.SELL, 100, 10);
                        }
                    }

                    for (int clientId = clientNum / 2; clientId < clientNum; clientId++) {
                        for (int tickerId = 0; tickerId < tickerNum; tickerId++) {
                            OrderRequest orderRequest = getOrderRequest(newOrderRequests, clientId, tickerId);
                            sendNewOrderRequest(orderRequest, clientId, tickerId, Side.BUY, 30, 10);
                        }
                    }

                    for (int i = 0; i < newOrderRequests.length; i++) {
                        OrderRequest newOrderRequest = newOrderRequests[i];
                        OrderRequest cancelOrderRequest = cancelOrderRequests[i];

                        sendCancelOrderRequest(cancelOrderRequest, newOrderRequest);
                    }

                    sleep(sleepTime);
                }

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
    }

    private void sendCancelOrderRequest(OrderRequest cancelOrderRequest, OrderRequest newOrderRequest) {
        cancelOrderRequest.setType(OrderRequestType.CANCEL);
        cancelOrderRequest.setClientId(newOrderRequest.getClientId());
        cancelOrderRequest.setTickerId(newOrderRequest.getTickerId());
        cancelOrderRequest.setOrderId(newOrderRequest.getOrderId());
        cancelOrderRequest.setSide(newOrderRequest.getSide());
        cancelOrderRequest.setQty(newOrderRequest.getQty());
        cancelOrderRequest.setPrice(newOrderRequest.getPrice());
        tradeEngine.sendOrderRequest(cancelOrderRequest);
    }

    private OrderRequest getOrderRequest(OrderRequest[] orderRequests, int clientId, int tickerId) {
        try {
            if (clientId < 0 || clientId >= clientNum) {
                log.error("Invalid clientId: {}. clientNum: {}", clientId, clientNum);
                return null;
            }
            if (tickerId < 0 || tickerId >= tickerNum) {
                log.error("Invalid tickerId: {}. tickerNum: {}", tickerId, tickerNum);
                return null;
            }
            return orderRequests[clientId * tickerNum + tickerId];
        } catch (Exception e) {
            log.error("Error in getOrderRequest clientId " + clientId + ", tickerId " + tickerId, e);
            throw new RuntimeException(e);
        }
    }

    private int rand(int bound) {
        return random.nextInt(bound);
    }

    private void sendRequest(int clientId, Side side, int qty, int price) {
        OrderRequest orderRequest = new OrderRequest();
        orderRequest.setType(OrderRequestType.NEW);
        orderRequest.setClientId(clientId);
        orderRequest.setTickerId(0);

        orderRequest.setOrderId(nextOrderId.getAndIncrement());

        orderRequest.setSide(side);
        orderRequest.setPrice(price);
        orderRequest.setQty(qty);

        orderGatewayClient.doSendOrderRequest(orderRequest);
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

    private void sendRandomSellBuyEverySec() {
        sleep(1_000);

        int qty = 50 + rand(10);
        int price = 8 + rand(12);

        sendRequest(10 + rand(10), Side.SELL, qty, price);

        sleep(1_000);
        sendRequest(10 + rand(10), Side.BUY, qty, price - 6);
    }
}
