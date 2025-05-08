package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderResponse;
import trading.api.OrderRequestType;
import trading.api.Side;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static trading.api.OrderResponseType.ACCEPTED;
import static trading.api.OrderResponseType.CANCELED;
import static trading.api.OrderResponseType.CANCEL_REJECTED;
import static trading.api.OrderResponseType.REQUEST_REJECT;
import static fix.FixUtils.getTestTag;

public class TestAlgo_5_Failover extends TestAlgo {

    private static final Logger log = LoggerFactory.getLogger(TestAlgo_5_Failover.class);

    private volatile boolean requestResent;
    private volatile boolean testFinished;
    private ScheduledFuture<?> resendCron;

    public TestAlgo_5_Failover(TradeEngine tradeEngine) {
        super(tradeEngine);
        log.info("TradingAlgo. Init.");
    }

    @Override
    public void onOrderUpdate(OrderResponse orderResponse) {
        TestRequest orderRequest = getOrderRequest();
        synchronized (orderRequest) {
            long orderId = orderResponse.getClientOrderId();

            if (requestResent && !testFinished) {
                if (orderResponse.getClientOrderId() != orderRequest.getOrderId()) {
                    log.warn("Unexpected {}", orderResponse);
                    return;
                }
                String testTag = getTestTag(orderResponse.getClientOrderId());
                log.info("-------------------------------" + testTag+ " FAILOVER SUCCEEDED---------------------------------");
                log.info("onOrderUpdate. [{}]. Failover succeeded in [{}] ms. Received Ack. {}. {}",
                        testTag,
                        Duration.between(orderRequest.newOrderTime, LocalDateTime.now()).toMillis(),
                        orderResponse,
                        orderRequest);
                log.info("-------------------------------" + testTag+ " FAILOVER SUCCEEDED---------------------------------");
                requestResent = false;
                testFinished = true;
            }

            if (orderResponse.getType() == ACCEPTED) {
                log.info("{} onOrderUpdate. Received New Order ACK: {}", getTestTag(orderId), orderResponse);
                sendCancelOrder();
            } else if (orderResponse.getType() == REQUEST_REJECT) {
                /* Indicates New Order was Submitted successfully earlier. Thus, dup new order with the same clOrdId is rejected  */
                log.info("{} onOrderUpdate. Received New Order NACK: {}", getTestTag(orderId), orderResponse);
                sendCancelOrder();
            } else if (orderResponse.getType() == CANCELED) {
                log.info("{} onOrderUpdate. Received Cancel Order ACK: {}", getTestTag(orderId), orderResponse);
                sendNewOrder();
            } else if (orderResponse.getType() == CANCEL_REJECTED ) {
                /* Indicates order was canceled earlier, and it is not live anymore */
                log.info("{} onOrderUpdate. Received Cancel Order NACK: {}", getTestTag(orderId), orderResponse);
                sendNewOrder();
            } else {
                log.error("onOrderUpdate. Unexpected {}", orderResponse);
            }
        }
    }

    @Override
    public void run() {
        try {
            log.info("TestAlgo_5_Failover. run. Thread: {}", Thread.currentThread().getName());
            scheduleResendCheck();
            Thread.sleep(10_000);
            synchronized (getOrderRequest()) {
                // Initial New Order Request to kick off looping: all later requests are generated on exchange responses
                sendNewOrder();
            }
        } catch (Exception e) {
            log.error("Error in TestAlgo", e);
        }
    }

    protected void sendNewOrder() {
        TestRequest orderRequest = getOrderRequest();
        orderRequest.cancelOrderTime = null;
        orderRequest.newOrderTime = LocalDateTime.now();
        sendNewOrderRequest(orderRequest, 0, 0, Side.SELL, 100, 10);
    }

    protected void sendCancelOrder() {
        TestRequest orderRequest = getOrderRequest();
        orderRequest.setType(OrderRequestType.CANCEL);
        orderRequest.cancelOrderTime = LocalDateTime.now();
        sendOrderRequest(orderRequest);
    }

    private void scheduleResendCheck() {
        getScheduler().scheduleAtFixedRate(
                () -> {
                    if (testFinished) {
                        return;
                    }
                    try {
                        TestRequest orderRequest = getOrderRequest();
                        synchronized (orderRequest) {
                            if (orderRequest.newOrderTime == null) {
                                log.info("orderRequest.sendingTime == NULL");
                                return;
                            }
                            log.debug("{} CheckAge: {}", getTestTag(orderRequest.getOrderId()), orderRequest);
                            long ageMs = Duration.between(orderRequest.newOrderTime, LocalDateTime.now()).toMillis();
                            if (ageMs > 700) {
                                orderRequest.resendingTime = LocalDateTime.now();
                                sendOrderRequest(orderRequest);
                                if (!requestResent) {
                                    log.info("CheckAge. First Resending [{}]. Age: {}ms {}", getTestTag(orderRequest.getOrderId()), ageMs, orderRequest);
                                    requestResent = true;
                                } else {
                                    log.debug("CheckAge. Resending again [{}]. Age: {}ms {}",
                                            getTestTag(orderRequest.getOrderId()), ageMs, orderRequest);
                                }
                            }
                        }
                    } catch (Exception exception) {
                        log.error("CheckAge failed", exception);
                    }
                },
                2000, // initial delay
                100, // repeat interval
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void onOrderBookUpdate(long tickerId, long price, Side side, MarketOrderBook marketOrderBook) {
//        log.info("onOrderBookUpdate. tickerId: {}, price: {}, side: {}", tickerId, price, side);
    }

    @Override
    public void onTradeUpdate(MarketUpdate marketUpdate, MarketOrderBook marketOrderBook) {
//        log.info("onTradeUpdate. marketUpdate: {}", marketUpdate);
    }

}
