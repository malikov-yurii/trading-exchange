package trading.participant.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.OrderMessage;
import trading.api.OrderMessageType;
import trading.api.OrderRequest;
import trading.api.Side;
import trading.common.Utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static trading.api.OrderMessageType.ACCEPTED;
import static trading.api.OrderMessageType.CANCELED;
import static trading.api.OrderMessageType.CANCEL_REJECTED;
import static trading.api.OrderMessageType.REQUEST_REJECT;
import static trading.common.Utils.getTestTag;

public class TestAlgo_5_Failover extends TestAlgo {

    private static final Logger log = LoggerFactory.getLogger(TestAlgo_5_Failover.class);

    private AtomicReference<OrderRequest> resentOrderRequest = new AtomicReference<>();

    public TestAlgo_5_Failover(TradeEngine tradeEngine) {
        super(tradeEngine);
        log.info("TradingAlgo. Init.");
    }

    @Override
    public void onOrderUpdate(OrderMessage orderMessage) {
        OrderRequest newOrderReq = getNewOrderRequest();
        synchronized (newOrderReq) {
            long orderId = orderMessage.getClientOrderId();
            OrderMessageType type = orderMessage.getType();
            long newOrderId = newOrderReq.getOrderId();
            long cancelOrderId = newOrderReq.getOrderId();

            if (orderId == newOrderId && type == ACCEPTED) {
                log.info("{} onOrderUpdate. Received New Order Ack {}: {}", getTestTag(orderId), type, orderMessage);
                sendCancelOrder();
            } else if (orderId == newOrderId && type == REQUEST_REJECT) {
                /* Indicates New Order was Submitted successfully earlier. Thus, dup new order with the same clOrdId is rejected  */
                log.info("{} onOrderUpdate. Received New Order Nack {}: {}", getTestTag(orderId), type, orderMessage);
                sendCancelOrder();
            } else if (orderId == cancelOrderId && (type == CANCELED
                    || type == CANCEL_REJECTED /* Indicates order was canceled earlier, and it is not live anymore */)) {

                OrderRequest resentRequest = resentOrderRequest.get();
                if (resentRequest != null && resentRequest.getOrderId() == orderId) {
                    log.info("onOrderUpdate. First Resend succeeded [{}]. Received Cancel Order Response {}: {}. " +
                            "Current Resent: {}", getTestTag(orderId), type, orderMessage, resentRequest);
                    resentOrderRequest.set(null);
                } else {
                    log.info("{} onOrderUpdate. Received Cancel Order Response {}: {}. " +
                            "Current Resent: {}", getTestTag(orderId), type, orderMessage, resentRequest);
                }

                sendNewOrder();
            } else {
                log.error("onOrderUpdate. Unexpected {}", orderMessage);
            }
        }
    }

    @Override
    public void run() {
        try {
            log.info("TestAlgo_5_Failover. run. Thread: {}", Thread.currentThread().getName());
            scheduleResendCheck();
            Thread.sleep(10_000);
            synchronized (getNewOrderRequest()) {
                // Initial New Order Request to kick off looping: all later requests are generated on exchange responses
                sendNewOrder();
            }
        } catch (Exception e) {
            log.error("Error in TestAlgo", e);
        }
    }

    private void scheduleResendCheck() {
        int timeoutMs = 100;
        int repeatIntervalMs = 100;
        getScheduler().scheduleAtFixedRate(
                () -> {
                    try {
                        OrderRequest newOrderReq = getNewOrderRequest();
                        synchronized (newOrderReq) {
                            TestRequest testRequest = getLastRequest().get();
                            if (testRequest == null) {
                                return;
                            }
                            OrderRequest last = testRequest.request();
                            if (last == null) {
                                log.info("Last == NULL");
                                return;
                            }
                            log.info("{} CheckAge: {}", Utils.getTestTag(last.getOrderId()), testRequest);
                            long age = Duration.between(testRequest.sent(), LocalDateTime.now()).toMillis();
                            if (age > timeoutMs) {
                                if (last == newOrderReq || last == getCancelOrderRequest()) {
                                    sendOrderRequest(last);
                                    boolean wasSet = resentOrderRequest.compareAndSet(null, last);
                                    if (wasSet) {
                                        log.info("CheckAge. First Resend init [{}]. Age: {}ms {}", Utils.getTestTag(last.getOrderId()), age, last);
                                    } else {
                                        log.info("CheckAge. Resend without tracking [{}]. Age: {}ms {}", Utils.getTestTag(last.getOrderId()), age, last);
                                    }
                                } else {
                                    log.error("CheckAge. Resend [{}]. Failed.", Utils.getTestTag(last.getOrderId()));
                                }
                            }
                        }
                    } catch (Exception exception) {
                        log.error("CheckAge failed", exception);
                    }
                },
                2000, // initial delay
                repeatIntervalMs, // repeat interval
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
