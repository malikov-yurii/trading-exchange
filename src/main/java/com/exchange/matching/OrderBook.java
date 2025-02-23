package com.exchange.matching;

import com.exchange.MEConstants;
import com.exchange.api.MarketUpdate;
import com.exchange.api.MarketUpdateType;
import com.exchange.api.OrderMessageType;
import com.exchange.api.OrderMessage;
import com.exchange.api.Side;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.exchange.MEConstants.PRIORITY_INVALID;

public final class OrderBook {

    private static final Logger log = LoggerFactory.getLogger(OrderBook.class);

    private final long tickerId;
    private MatchingEngine matchingEngine;

    private OrdersAtPrice bidsByPrice;
    private OrdersAtPrice asksByPrice;

    private final ClientOrderMap clientOrdersMap;
    private final OrdersAtPriceMap ordersAtPriceMap;

    private OrderMessage orderMessage = new OrderMessage();
    private MarketUpdate marketUpdate = new MarketUpdate();

    private long nextMarketOrderId = 1;

    public OrderBook(long tickerId, MatchingEngine matchingEngine) {
        this.tickerId = tickerId;
        this.matchingEngine = matchingEngine;
        clientOrdersMap = new ClientOrderMap(MEConstants.ME_MAX_NUM_CLIENTS, MEConstants.ME_MAX_ORDER_IDS);
        ordersAtPriceMap = new OrdersAtPriceMap(MEConstants.ME_MAX_PRICE_LEVELS);
    }

    public void close() {
        log.info("Closing {}. ", this);
        matchingEngine = null;
        bidsByPrice = null;
        asksByPrice = null;
    }

    public void add(long clientId, long clientOrderId, long tickerId, Side side, long price, long qty) {
        long marketOrderId = generateNewMarketOrderId();

        orderMessage = new OrderMessage(OrderMessageType.ACCEPTED, clientId, tickerId, clientOrderId, marketOrderId, side, price, 0, qty);
        matchingEngine.sendClientResponse(orderMessage);

        long leavesQty = matchNewOrder(clientId, clientOrderId, tickerId, side, price, qty, marketOrderId);

        if (leavesQty > 0) {
            long priority = getNextPriority(price);
            Order order = new Order(tickerId, clientId, clientOrderId, marketOrderId, side, price, leavesQty, priority, null, null);
            addOrder(order);

            marketUpdate = new MarketUpdate(MarketUpdateType.ADD, marketOrderId, tickerId, side, price, leavesQty, priority);
            matchingEngine.sendMarketUpdate(marketUpdate);
        }
    }

    public void cancel(long clientId, long clientOrderId, long tickerId) {
        Order order = clientOrdersMap.get(clientId, clientOrderId);

        if (order == null) {
            orderMessage = new OrderMessage(OrderMessageType.CANCEL_REJECTED, clientId, tickerId, clientOrderId,
                    MEConstants.ORDER_ID_INVALID, Side.INVALID, MEConstants.PRICE_INVALID, MEConstants.QTY_INVALID, MEConstants.QTY_INVALID);
        } else {
            orderMessage = new OrderMessage(OrderMessageType.CANCELED, clientId, tickerId, clientOrderId,
                    order.getMarketOrderId(), order.getSide(), order.getPrice(), MEConstants.QTY_INVALID, order.getQty());
            marketUpdate = new MarketUpdate(MarketUpdateType.CANCEL, order.getMarketOrderId(), tickerId,
                    order.getSide(), order.getPrice(), 0, order.getPriority());

            removeOrder(order);
            matchingEngine.sendMarketUpdate(marketUpdate);
        }

        matchingEngine.sendClientResponse(orderMessage);
    }

    // Checks for a match between the new order and the existing orders in the order book.
    private long matchNewOrder(long clientId, long clientOrderId, long tickerId, Side side,
                               long price, long qty, long newMarketOrderId) {
        long leavesQty = qty;
        if (side == Side.BUY) {
            while (leavesQty > 0 && asksByPrice != null) {
                Order askItr = asksByPrice.getFirstOrder();
                if (price < askItr.getPrice()) {
                    break;
                }
                leavesQty = match(tickerId, clientId, side, clientOrderId, newMarketOrderId, askItr, leavesQty);
            }
        } else if (side == Side.SELL) {
            while (leavesQty > 0 && bidsByPrice != null) {
                Order bidItr = bidsByPrice.getFirstOrder();
                if (price > bidItr.getPrice()) {
                    break;
                }
                leavesQty = match(tickerId, clientId, side, clientOrderId, newMarketOrderId, bidItr, leavesQty);
            }
        }
        return leavesQty;
    }

    private long match(long tickerId, long clientId, Side side, long clientOrderId, long newMarketOrderId,
                       Order passiveOrder, long leavesQty) {
        long orderQty = passiveOrder.getQty();
        long fillQty = Math.min(leavesQty, orderQty);

        leavesQty -= fillQty;
        passiveOrder.setQty(orderQty - fillQty);

        orderMessage = new OrderMessage(OrderMessageType.FILLED, clientId, tickerId, clientOrderId,
                newMarketOrderId, side, passiveOrder.getPrice(), fillQty, leavesQty);
        matchingEngine.sendClientResponse(orderMessage); // Fill for the aggressive order

        orderMessage = new OrderMessage(OrderMessageType.FILLED, passiveOrder.getClientId(), tickerId,
                passiveOrder.getClientOrderId(), passiveOrder.getMarketOrderId(), passiveOrder.getSide(),
                passiveOrder.getPrice(), fillQty, passiveOrder.getQty());
        matchingEngine.sendClientResponse(orderMessage); // Fill for the passive order

        marketUpdate = new MarketUpdate(MarketUpdateType.TRADE, MEConstants.ORDER_ID_INVALID, tickerId, side,
                passiveOrder.getPrice(), fillQty, PRIORITY_INVALID);
        matchingEngine.sendMarketUpdate(marketUpdate);

        if (passiveOrder.getQty() == 0) { // fully matched
            marketUpdate = new MarketUpdate(MarketUpdateType.CANCEL, passiveOrder.getMarketOrderId(), tickerId,
                    passiveOrder.getSide(), passiveOrder.getPrice(), orderQty, PRIORITY_INVALID);
            matchingEngine.sendMarketUpdate(marketUpdate);

            removeOrder(passiveOrder);
        } else {
            marketUpdate = new MarketUpdate(MarketUpdateType.MODIFY, passiveOrder.getMarketOrderId(), tickerId,
                    passiveOrder.getSide(), passiveOrder.getPrice(), passiveOrder.getQty(), passiveOrder.getPriority());
            matchingEngine.sendMarketUpdate(marketUpdate);
        }
        return leavesQty;
    }

    private void addOrder(Order order) {
        OrdersAtPrice ordersAtPrice = ordersAtPriceMap.get(order.getPrice());
        if (ordersAtPrice == null) {
            // Create new
            order.setNextOrder(order);
            order.setPrevOrder(order);
            OrdersAtPrice newNode = new OrdersAtPrice(order.getSide(), order.getPrice(), order, null, null);
            ordersAtPriceMap.put(newNode);

            if (order.getSide() == Side.BUY) {
                if (bidsByPrice == null) {
                    bidsByPrice = newNode;
                    newNode.setPrev(newNode);
                    newNode.setNext(newNode);
                } else {
                    insertPriceLevel(bidsByPrice, newNode, true);
                }
            } else {
                if (asksByPrice == null) {
                    asksByPrice = newNode;
                    newNode.setPrev(newNode);
                    newNode.setNext(newNode);
                } else {
                    insertPriceLevel(asksByPrice, newNode, false);
                }
            }
        } else {
            // Insert into existing FIFO queue at this price
            Order firstOrder = ordersAtPrice.getFirstOrder();
            Order lastOrder = firstOrder.getPrevOrder();

            lastOrder.setNextOrder(order);
            order.setPrevOrder(lastOrder);
            order.setNextOrder(firstOrder);
            firstOrder.setPrevOrder(order);
        }

        clientOrdersMap.put(order);
    }

    private void removeOrder(Order order) {
        OrdersAtPrice ordersAtPrice = ordersAtPriceMap.get(order.getPrice());
        if (ordersAtPrice == null) {
            return;
        }

        if (order.getPrevOrder() == order) {
            // Single order in this price level
            removeOrdersAtPrice(order.getSide(), order.getPrice());
        } else {
            Order prev = order.getPrevOrder();
            Order next = order.getNextOrder();
            prev.setNextOrder(next);
            next.setPrevOrder(prev);

            if (ordersAtPrice.getFirstOrder() == order) {
                ordersAtPrice.setFirstOrder(next);
            }
            order.setPrevOrder(null);
            order.setNextOrder(null);
        }

        clientOrdersMap.remove(order);
    }

    private void removeOrdersAtPrice(Side side, long price) {
        OrdersAtPrice node = ordersAtPriceMap.get(price);
        if (node == null) {
            return;
        }

        if (node.getNext() == node) {
            // Single price level
            if (side == Side.BUY) {
                bidsByPrice = null;
            } else {
                asksByPrice = null;
            }
        } else {
            OrdersAtPrice prev = node.getPrev();
            OrdersAtPrice next = node.getNext();
            prev.setNext(next);
            next.setPrev(prev);

            if (side == Side.BUY && bidsByPrice == node) {
                bidsByPrice = next;
            } else if (side == Side.SELL && asksByPrice == node) {
                asksByPrice = next;
            }
        }
        ordersAtPriceMap.remove(price);
    }

    // Insert newNode into the doubly linked list of price levels.
    private void insertPriceLevel(OrdersAtPrice best, OrdersAtPrice newNode, boolean isBuy) {
        // If the new node is "better" than the current best, insert it as the new best.
        // For buys, a "better" price is higher. For sells, a "better" price is lower.
        if ((isBuy && newNode.getPrice() > best.getPrice()) ||
                (!isBuy && newNode.getPrice() < best.getPrice())) {

            // Insert newNode before 'best' in the ring
            OrdersAtPrice prevBest = best.getPrev();
            prevBest.setNext(newNode);
            newNode.setPrev(prevBest);
            newNode.setNext(best);
            best.setPrev(newNode);

            if (isBuy) {
                bidsByPrice = newNode;
            } else {
                asksByPrice = newNode;
            }
            return;
        }

        // For buys: we move forward while newNode's price is <= the next node's price (descending).
        // For sells: we move forward while newNode's price is >= the next node's price (ascending).
        OrdersAtPrice current = best;
        while (true) {
            OrdersAtPrice next = current.getNext();
            if (next == best || isBuy ? newNode.getPrice() > next.getPrice() : newNode.getPrice() < next.getPrice()) {
                // Insert 'newNode' after 'current' and before 'next'
                newNode.setNext(next);
                newNode.setPrev(current);
                current.setNext(newNode);
                next.setPrev(newNode);
                return;
            }

            current = next; // move forward in the ring
        }
    }

    private long getNextPriority(long price) {
        OrdersAtPrice node = ordersAtPriceMap.get(price);
        return node == null ? 1 : node.getFirstOrder().getPrevOrder().getPriority() + 1;
    }

    private long generateNewMarketOrderId() {
        return nextMarketOrderId++;
    }

    @Override
    public String toString() {
        return "MEOrderBook[Ticker:" + tickerId + "]";
    }

    private void fatal(String msg) {
        throw new RuntimeException(msg);
    }

}