package trading.participant.strategy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.Side;
import trading.common.Constants;

public class MarketOrderBook {

    private static final Logger log = LoggerFactory.getLogger(MarketOrderBook.class);

    @Getter
    private final long tickerId;

    private final BBO bbo;

    private final TradeEngine tradeEngine;

    private final MarketOrder[] marketOrdersById = new MarketOrder[Constants.ME_MAX_ORDER_IDS];

    private MarketOrdersAtPrice bidsByPrice = null;
    private MarketOrdersAtPrice asksByPrice = null;

    private final MarketOrdersAtPrice[] marketOrdersAtPriceByPrice = new MarketOrdersAtPrice[Constants.ME_MAX_PRICE_LEVELS];

    public MarketOrderBook(long tickerId, TradeEngine tradeEngine) {
        this.tickerId = tickerId;
        this.tradeEngine = tradeEngine;
        this.bbo = new BBO();
    }

    public void onMarketUpdate(MarketUpdate marketUpdate) {
        try {
//            log.info("onMarketUpdate. {}", marketUpdate);
            if (marketUpdate.getTickerId() != tickerId) {
                log.error("onMarketUpdate. Invalid tickerId: {}. Order book tikerId: {}", marketUpdate.getTickerId(), tickerId);
                return;
            }
            switch (marketUpdate.getType()) {
                case ADD:
//                    log.info("onMarketUpdate. Before addOrder: {}", marketUpdate);
                    addOrder(marketUpdate);
//                    log.info("onMarketUpdate. After addOrder: {}", marketUpdate);
                    break;
                case MODIFY:
                    MarketOrder marketOrder = marketOrdersById[(int) marketUpdate.getOrderId()];
                    marketOrder.setQty(marketUpdate.getQty());
                    break;
                case CANCEL:
                    removeOrder(marketUpdate);
                    break;
                case TRADE:
                    tradeEngine.onTradeUpdate(this, marketUpdate);
                    break;
                case CLEAR:
                    // TODO Clear the order book and deallocate MarketOrdersAtPrice and MarketOrder objects.
                    break;
                default:
                    log.error("onMarketUpdate. Invalid MarketUpdateType: {}", marketUpdate.getType());
            }
            boolean bidUpdated = bidsByPrice != null && marketUpdate.getSide() == Side.BUY
                    && marketUpdate.getPrice() >= bidsByPrice.getPrice();
            boolean askUpdated = asksByPrice != null && marketUpdate.getSide() == Side.SELL
                    && marketUpdate.getPrice() <= asksByPrice.getPrice();

//            log.info("onMarketUpdate. Before updateBBO: {}", marketUpdate);
            updateBBO(bidUpdated, askUpdated);

//            log.info("onMarketUpdate. Before tradeEngine.onOrderBookUpdate: {}", marketUpdate);
            tradeEngine.onOrderBookUpdate(this, marketUpdate.getPrice(), marketUpdate.getSide());
//            log.info("onMarketUpdate. Finished. {}", marketUpdate);
        } catch (Exception e) {
            log.error("Error processing MarketUpdate: {}", marketUpdate, e);
        }
    }

    private void updateBBO(boolean bidUpdated, boolean askUpdated) {
//        log.info("updateBBO. Start bidUpdated: {}, askUpdated: {}", bidUpdated, askUpdated);
        if (bidUpdated) {
            if (bidsByPrice != null) {
                bbo.setBidPrice(bidsByPrice.getPrice());
                MarketOrder order = bidsByPrice.getFirstOrder();
                long qty = order.getQty();
                while (order != order.getNextOrder()) {
                    order = order.getNextOrder();
                    qty += order.getQty();
                }
                bbo.setBidQty(qty);
            } else {
                bbo.setBidPrice(Constants.PRICE_INVALID);
                bbo.setBidQty(Constants.QTY_INVALID);
            }
        }

        if (askUpdated) {
            if (asksByPrice != null) {
                bbo.setAskPrice(asksByPrice.getPrice());
                MarketOrder order = asksByPrice.getFirstOrder();
                long qty = order.getQty();
                while (order != order.getNextOrder()) {
                    order = order.getNextOrder();
                    qty += order.getQty();
                }
                bbo.setAskQty(qty);
            } else {
                bbo.setAskPrice(Constants.PRICE_INVALID);
                bbo.setAskQty(Constants.QTY_INVALID);
            }
        }
//        log.info("updateBBO. Finish bidUpdated: {}, askUpdated: {}", bidUpdated, askUpdated);
    }

    private void removeOrder(MarketUpdate marketUpdate) {
        MarketOrder marketOrder = marketOrdersById[(int) marketUpdate.getOrderId()];
        MarketOrdersAtPrice marketOrdersAtPrice = getMarketOrdersAtPrice(marketUpdate.getPrice());
        if (marketOrdersAtPrice.getFirstOrder() == marketOrdersAtPrice.getFirstOrder().getNextOrder()) {
            removeMarketOrdersAtPrice(marketOrdersAtPrice);
        } else {
            if (marketOrdersAtPrice.getFirstOrder() == marketOrder) {
                marketOrdersAtPrice.setFirstOrder(marketOrder.getNextOrder());
            }
            marketOrder.getPrevOrder().setNextOrder(marketOrder.getNextOrder());
            marketOrder.getNextOrder().setPrevOrder(marketOrder.getPrevOrder());
        }
    }

    private void removeMarketOrdersAtPrice(MarketOrdersAtPrice marketOrdersAtPrice) {
        if (marketOrdersAtPrice.getSide() == Side.BUY) {
            if (marketOrdersAtPrice == bidsByPrice) {
                bidsByPrice = marketOrdersAtPrice.getNext();
            }
        } else {
            if (marketOrdersAtPrice == asksByPrice) {
                asksByPrice = marketOrdersAtPrice.getNext();
            }
        }
        marketOrdersAtPrice.getPrev().setNext(marketOrdersAtPrice.getNext());
        marketOrdersAtPrice.getNext().setPrev(marketOrdersAtPrice.getPrev());
    }

    private void addOrder(MarketUpdate marketUpdate) {
//        log.info("Adding order: {}", marketUpdate);
        MarketOrder marketOrder = new MarketOrder(marketUpdate.getTickerId(), marketUpdate.getOrderId(),
                marketUpdate.getSide(), marketUpdate.getPrice(), marketUpdate.getQty(), marketUpdate.getPriority(),
                null, null);

        marketOrdersById[(int) marketUpdate.getOrderId()] = marketOrder;


        MarketOrdersAtPrice marketOrdersAtPrice = getMarketOrdersAtPrice(marketUpdate.getPrice());
        if (marketOrdersAtPrice == null) {
            marketOrdersAtPrice = new MarketOrdersAtPrice(marketOrder);
            addMarketOrderAtPrice(marketOrdersAtPrice);
        }
        marketOrdersAtPrice.getFirstOrder().getPrevOrder().setNextOrder(marketOrder);
        marketOrder.setPrevOrder(marketOrdersAtPrice.getFirstOrder().getPrevOrder());
        marketOrdersAtPrice.getFirstOrder().setPrevOrder(marketOrder);
        marketOrder.setNextOrder(marketOrdersAtPrice.getFirstOrder());
        log.info("Added order: {}", marketOrder);
    }

    private MarketOrdersAtPrice getMarketOrdersAtPrice(long price) {
        MarketOrdersAtPrice marketOrdersAtPrice = marketOrdersAtPriceByPrice[priceToIndex(price)];
        return marketOrdersAtPrice;
    }

    private void addMarketOrderAtPrice(MarketOrdersAtPrice newOrdersAtPrice) {
        marketOrdersAtPriceByPrice[priceToIndex(newOrdersAtPrice.getPrice())] = newOrdersAtPrice;
        MarketOrdersAtPrice bestOfferOrders = newOrdersAtPrice.getSide() == Side.BUY ? bidsByPrice : asksByPrice;
        if (bestOfferOrders == null) {
            if (newOrdersAtPrice.getSide() == Side.BUY) {
                bidsByPrice = newOrdersAtPrice;
            } else {
                asksByPrice = newOrdersAtPrice;
            }
            newOrdersAtPrice.setNext(newOrdersAtPrice);
            newOrdersAtPrice.setPrev(newOrdersAtPrice);
            return;
        }
        MarketOrdersAtPrice target = bestOfferOrders;

        boolean addAfterTarget = target.hasBetterPriceThan(newOrdersAtPrice.getPrice());
        if (addAfterTarget) {
            while (target.getNext() != bestOfferOrders && target.getNext().hasBetterPriceThan(newOrdersAtPrice.getPrice())) {
                target = target.getNext();
            }
        }
        if (addAfterTarget) {
            newOrdersAtPrice.setNext(target.getNext());
            newOrdersAtPrice.setPrev(target);
            target.getNext().setPrev(newOrdersAtPrice);
            target.setNext(newOrdersAtPrice);
        } else {
            newOrdersAtPrice.setNext(target);
            newOrdersAtPrice.setPrev(target.getPrev());
            target.getPrev().setNext(newOrdersAtPrice);
            target.setPrev(newOrdersAtPrice);
            if (newOrdersAtPrice.getSide() == Side.BUY) {
                bidsByPrice = newOrdersAtPrice;
            } else {
                asksByPrice = newOrdersAtPrice;
            }
        }
    }

    public int priceToIndex(long price) {
        return (int) (price / Constants.ME_MAX_PRICE_LEVELS);
    }

    public BBO getBBO() {
        return bbo;
    }

    /**
     * A price level in the limit order book.
     */
    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    private static class MarketOrdersAtPrice {

        private Side side;
        private long price;

        private MarketOrder firstOrder;
        private MarketOrdersAtPrice prev;
        private MarketOrdersAtPrice next;


        public MarketOrdersAtPrice(MarketOrder marketOrder) {
            this.side = marketOrder.getSide();
            this.price = marketOrder.getPrice();
            this.firstOrder = marketOrder;

            marketOrder.setPrevOrder(marketOrder);
            marketOrder.setNextOrder(marketOrder);
        }

        public boolean hasBetterPriceThan(long price) {
            return Side.BUY.equals(side) ? this.price > price : this.price < price;
        }

        @Override
        public String toString() {
            return "MarketOrdersAtPrice{" +
                    "firstOrder=" + firstOrder +
//                    ", next=" + next +
//                    ", prev=" + prev +
                    ", price=" + price +
                    ", side=" + side +
                    '}';
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class MarketOrder {

        private long tickerId;
        private long orderId;
        private Side side;
        private long price;
        private long qty;
        private long priority;
        private MarketOrder prevOrder;
        private MarketOrder nextOrder;

        @Override
        public String toString() {
            return "MarketOrder{" +
//                    "nextOrder=" + nextOrder +
                    ", orderId=" + orderId +
//                    ", prevOrder=" + prevOrder +
                    ", price=" + price +
                    ", priority=" + priority +
                    ", qty=" + qty +
                    ", side=" + side +
                    ", tickerId=" + tickerId +
                    '}';
        }
    }

    @Getter
    @Setter
    public static class BBO {

        private long bidPrice;
        private long askPrice;
        private long bidQty;
        private long askQty;

        @Override
        public String toString() {
            return "BBO{" +
                    "askPrice=" + askPrice +
                    ", askQty=" + askQty +
                    ", bidPrice=" + bidPrice +
                    ", bidQty=" + bidQty +
                    '}';
        }
    }
}
