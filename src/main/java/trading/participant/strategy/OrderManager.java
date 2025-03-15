package trading.participant.strategy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderMessage;
import trading.api.OrderRequest;
import trading.api.OrderRequestType;
import trading.api.Side;
import trading.common.Constants;

import java.util.concurrent.atomic.AtomicLong;

public class OrderManager {
    private static final Logger log = LoggerFactory.getLogger(OrderManager.class);

    private final TradeEngine tradeEngine;
    private final RiskManager riskManager;
    private final TickerOrderSideMap tickerOrderSideMap;
    private final AtomicLong nextOrderId;


    public OrderManager(TradeEngine tradeEngine, RiskManager riskManager) {
        this.tradeEngine = tradeEngine;
        this.riskManager = riskManager;
        this.tickerOrderSideMap = new TickerOrderSideMap();
        this.nextOrderId = new AtomicLong();
    }

    public void onOrderMessage(OrderMessage orderMessage) {
        Order order;
        switch (orderMessage.getType()) {
            case ACCEPTED:
                order = tickerOrderSideMap.getOrder(orderMessage.getTickerId(), orderMessage.getSide());
                order.setOrderStatus(OrderStatus.LIVE);
                break;
            case CANCELED:
                order = tickerOrderSideMap.getOrder(orderMessage.getTickerId(), orderMessage.getSide());
                order.setOrderStatus(OrderStatus.DEAD);
                break;
            case FILLED:
                order = tickerOrderSideMap.getOrder(orderMessage.getTickerId(), orderMessage.getSide());
                order.setQty(orderMessage.getLeavesQty());
                if (order.getQty() == 0) {
                    order.setOrderStatus(OrderStatus.DEAD);
                }
                break;
            case CANCEL_REJECTED:
            case INVALID:
            default:
                log.info("Received message type: {}", orderMessage.getType());
        }
    }

    public void moveOrder(Order order, long tickerId, long price, Side side, long qty) {
        if (price == Constants.PRICE_INVALID || qty == Constants.QTY_INVALID) {
            return;
        }
        switch (order.getOrderStatus()) {
            case LIVE:
                if (order.getPrice() != price) {
                    cancelOrder(order);
                }
                break;
            case INVALID:
            case DEAD:
                RiskInfo.RiskCheckResult riskCheckResult = riskManager.checkPreTradeRisk(tickerId, side, qty);
                if (riskCheckResult == RiskInfo.RiskCheckResult.RISK_ALLOWED) {
                    newOrder(order, tickerId, price, side, qty);
                } else {
                    log.info("Risk check failed for tickerId: {}, {} {}@{}. Result: {}",
                            tickerId, side, qty, price, riskCheckResult);
                }
                break;
            case PENDING_NEW:
            case PENDING_CANCEL:
            default:
                break;
        }
    }

    public void moveOrders(long tickerId, long bidPrice, long askPrice, long qty) {
        log.info("moveOrders. tickerId={}, bidPrice={}, askPrice={}, qty={}", tickerId, bidPrice, askPrice, qty);
        Order bidOrder = tickerOrderSideMap.getOrder(tickerId, Side.BUY);
        Order askOrder = tickerOrderSideMap.getOrder(tickerId, Side.SELL);

        moveOrder(bidOrder, tickerId, bidPrice, Side.BUY, qty);
        moveOrder(askOrder, tickerId, askPrice, Side.SELL, qty);
    }

    private void newOrder(Order order, long tickerId, long price, Side side, long qty) {
        OrderRequest orderRequest = new OrderRequest();
        orderRequest.setType(OrderRequestType.NEW);
        orderRequest.setClientId(tradeEngine.getClientId());
        orderRequest.setTickerId(tickerId);
        orderRequest.setOrderId(nextOrderId.getAndIncrement());
        orderRequest.setSide(side);
        orderRequest.setPrice(price);
        orderRequest.setQty(qty);

        tradeEngine.sendOrderRequest(orderRequest);

        order.setTickerId(tickerId);
        order.setOrderId(orderRequest.getOrderId());
        order.setSide(side);
        order.setPrice(price);
        order.setQty(qty);
        order.setOrderStatus(OrderStatus.PENDING_NEW);
    }

    private void cancelOrder(Order order) {
        OrderRequest orderRequest = new OrderRequest();
        orderRequest.setType(OrderRequestType.CANCEL);
        orderRequest.setClientId(tradeEngine.getClientId());
        orderRequest.setTickerId(order.getTickerId());
        orderRequest.setOrderId(order.getOrderId());
        orderRequest.setSide(order.getSide());
        orderRequest.setPrice(order.getPrice());
        orderRequest.setQty(order.getQty());
        tradeEngine.sendOrderRequest(orderRequest);
        order.setOrderStatus(OrderStatus.PENDING_CANCEL);
    }


    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Order {
        private long tickerId = Constants.TICKER_ID_INVALID;
        private long orderId = Constants.ORDER_ID_INVALID;
        private Side side = Side.INVALID;
        private long price = Constants.PRICE_INVALID;
        private long qty = Constants.QTY_INVALID;
        private OrderStatus orderStatus = OrderStatus.INVALID;

        @Override
        public String toString() {
            return "Order{" +
                    "orderId=" + orderId +
                    ", orderStatus=" + orderStatus +
                    ", price=" + price +
                    ", qty=" + qty +
                    ", side=" + side +
                    ", tickerId=" + tickerId +
                    '}';
        }
    }

    private static class TickerOrderSideMap {
        private final OrderSideMap[] orders;

        public TickerOrderSideMap() {
            this.orders = new OrderSideMap[Constants.ME_MAX_TICKERS];
            for (int tickerId = 0; tickerId < orders.length; tickerId++) {
                orders[tickerId] = new OrderSideMap(tickerId);
            }
        }

        public Order getOrder(long tickerId, Side side) {
            return orders[tickerToIndex(tickerId)].getOrder(side);
        }

        private static int tickerToIndex(long tickerId) {
            return (int) (tickerId % Constants.ME_MAX_TICKERS);
        }

        private static class OrderSideMap {
            private final Order[] orders;

            public OrderSideMap(int tickerId) {
                this.orders = new Order[Side.getMaxSides()];
                for (int i = 0; i < orders.length; i++) {
                    orders[i] = new Order();
                    orders[i].setTickerId(tickerId);
                }
            }

            public Order getOrder(Side side) {
                return orders[side.toIndex()];
            }
        }


    }

    public enum OrderStatus {

        INVALID,
        PENDING_NEW,
        LIVE,
        PENDING_CANCEL,
        DEAD;

    }
}
