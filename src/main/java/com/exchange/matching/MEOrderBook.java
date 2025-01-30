package com.exchange.matching;

import com.exchange.MEConstants;
import com.exchange.marketdata.MEMarketUpdate;
import com.exchange.marketdata.MarketUpdateType;
import com.exchange.orderserver.ClientResponseType;
import com.exchange.orderserver.MEClientResponse;
import com.exchange.orderserver.Side;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.exchange.MEConstants.PRIORITY_INVALID;

public final class MEOrderBook {

    private static final Logger log = LoggerFactory.getLogger(MEOrderBook.class);

    private final long tickerId;
    private MatchingEngine matchingEngine;

    private MEOrdersAtPrice bidsByPrice;
    private MEOrdersAtPrice asksByPrice;

    private final ClientOrderHashMap clientOrders;
    private final OrdersAtPriceMap priceOrdersAtPrice;

    private MEClientResponse clientResponse = new MEClientResponse();
    private MEMarketUpdate marketUpdate = new MEMarketUpdate();

    private long nextMarketOrderId = 1;

    public MEOrderBook(long tickerId, MatchingEngine matchingEngine) {
        this.tickerId = tickerId;
        this.matchingEngine = matchingEngine;
        clientOrders = new ClientOrderHashMap(MEConstants.ME_MAX_NUM_CLIENTS, MEConstants.ME_MAX_ORDER_IDS);
        priceOrdersAtPrice = new OrdersAtPriceMap(MEConstants.ME_MAX_PRICE_LEVELS);
    }

    public void close() {
        log.info("Closing {}. ", this);
        matchingEngine = null;
        bidsByPrice = null;
        asksByPrice = null;
    }

    public void add(long clientId, long clientOrderId, long tickerId, Side side, long price, long qty) {
        long marketOrderId = generateNewMarketOrderId();

        clientResponse = new MEClientResponse(ClientResponseType.ACCEPTED, clientId, tickerId, clientOrderId, marketOrderId, side, price, 0, qty);
        matchingEngine.sendClientResponse(clientResponse);

        long leavesQty = matchNewOrder(clientId, clientOrderId, tickerId, side, price, qty, marketOrderId);

        if (leavesQty > 0) {
            long priority = getNextPriority(price);
            MEOrder order = new MEOrder(tickerId, clientId, clientOrderId, marketOrderId, side, price, leavesQty, priority, null, null);
            addOrder(order);

            marketUpdate = new MEMarketUpdate(MarketUpdateType.ADD, marketOrderId, tickerId, side, price, leavesQty, priority);
            matchingEngine.sendMarketUpdate(marketUpdate);
        }
    }

    public void cancel(long clientId, long orderId, long tickerId) {
        MEOrder order = clientOrders.get(clientId, orderId);

        if (order == null) {
            clientResponse = new MEClientResponse(ClientResponseType.CANCEL_REJECTED, clientId, tickerId, orderId,
                    MEConstants.ORDER_ID_INVALID, Side.INVALID, MEConstants.PRICE_INVALID, MEConstants.QTY_INVALID, MEConstants.QTY_INVALID);
        } else {
            clientResponse = new MEClientResponse(ClientResponseType.CANCELED, clientId, tickerId, orderId,
                    order.getMarketOrderId(), order.getSide(), order.getPrice(), MEConstants.QTY_INVALID, order.getQty());
            marketUpdate = new MEMarketUpdate(MarketUpdateType.CANCEL, order.getMarketOrderId(), tickerId,
                    order.getSide(), order.getPrice(), 0, order.getPriority());

            removeOrder(order);
            matchingEngine.sendMarketUpdate(marketUpdate);
        }

        matchingEngine.sendClientResponse(clientResponse);
    }

    // Checks for a match between the new order and the existing orders in the order book.
    private long matchNewOrder(long clientId, long clientOrderId, long tickerId, Side side,
                               long price, long qty, long newMarketOrderId) {
        long leavesQty = qty;
        if (side == Side.BUY) {
            while (leavesQty > 0 && asksByPrice != null) {
                MEOrder askItr = asksByPrice.getFirstMeOrder();
                if (price < askItr.getPrice()) {
                    break;
                }
                leavesQty = match(tickerId, clientId, side, clientOrderId, newMarketOrderId, askItr, leavesQty);
            }
        } else if (side == Side.SELL) {
            while (leavesQty > 0 && bidsByPrice != null) {
                MEOrder bidItr = bidsByPrice.getFirstMeOrder();
                if (price > bidItr.getPrice()) {
                    break;
                }
                leavesQty = match(tickerId, clientId, side, clientOrderId, newMarketOrderId, bidItr, leavesQty);
            }
        }
        return leavesQty;
    }

    private long match(long tickerId, long clientId, Side side, long clientOrderId, long newMarketOrderId,
                       MEOrder passiveOrder, long leavesQty) {
        long orderQty = passiveOrder.getQty();
        long fillQty = Math.min(leavesQty, orderQty);

        leavesQty -= fillQty;
        passiveOrder.setQty(orderQty - fillQty);

        clientResponse = new MEClientResponse(ClientResponseType.FILLED, clientId, tickerId, clientOrderId,
                newMarketOrderId, side, passiveOrder.getPrice(), fillQty, leavesQty);
        matchingEngine.sendClientResponse(clientResponse); // Fill for the aggressive order

        clientResponse = new MEClientResponse(ClientResponseType.FILLED, passiveOrder.getClientId(), tickerId,
                passiveOrder.getClientOrderId(), passiveOrder.getMarketOrderId(), passiveOrder.getSide(),
                passiveOrder.getPrice(), fillQty, passiveOrder.getQty());
        matchingEngine.sendClientResponse(clientResponse); // Fill for the passive order

        marketUpdate = new MEMarketUpdate(MarketUpdateType.TRADE, MEConstants.ORDER_ID_INVALID, tickerId, side,
                passiveOrder.getPrice(), fillQty, PRIORITY_INVALID);
        matchingEngine.sendMarketUpdate(marketUpdate);

        if (passiveOrder.getQty() == 0) { // fully matched
            marketUpdate = new MEMarketUpdate(MarketUpdateType.CANCEL, passiveOrder.getMarketOrderId(), tickerId,
                    passiveOrder.getSide(), passiveOrder.getPrice(), orderQty, PRIORITY_INVALID);
            matchingEngine.sendMarketUpdate(marketUpdate);

            removeOrder(passiveOrder);
        } else {
            marketUpdate = new MEMarketUpdate(MarketUpdateType.MODIFY, passiveOrder.getMarketOrderId(), tickerId,
                    passiveOrder.getSide(), passiveOrder.getPrice(), passiveOrder.getQty(), passiveOrder.getPriority());
            matchingEngine.sendMarketUpdate(marketUpdate);
        }
        return leavesQty;
    }

    private void addOrder(MEOrder order) {
        MEOrdersAtPrice ordersAtPrice = priceOrdersAtPrice.get(order.getPrice());
        if (ordersAtPrice == null) {
            // Create new
            order.setNextOrder(order);
            order.setPrevOrder(order);
            MEOrdersAtPrice newNode = new MEOrdersAtPrice(order.getSide(), order.getPrice(), order, null, null);
            priceOrdersAtPrice.put(order.getPrice(), newNode);

            if (order.getSide() == Side.BUY) {
                if (bidsByPrice == null) {
                    bidsByPrice = newNode;
                    newNode.setPrevEntry(newNode);
                    newNode.setNextEntry(newNode);
                } else {
                    insertPriceLevel(bidsByPrice, newNode, true);
                }
            } else {
                if (asksByPrice == null) {
                    asksByPrice = newNode;
                    newNode.setPrevEntry(newNode);
                    newNode.setNextEntry(newNode);
                } else {
                    insertPriceLevel(asksByPrice, newNode, false);
                }
            }
        } else {
            // Insert into existing FIFO queue at this price
            MEOrder firstOrder = ordersAtPrice.getFirstMeOrder();
            MEOrder lastOrder = firstOrder.getPrevOrder();

            lastOrder.setNextOrder(order);
            order.setPrevOrder(lastOrder);
            order.setNextOrder(firstOrder);
            firstOrder.setPrevOrder(order);
        }

        clientOrders.put(order);
    }

    private void removeOrder(MEOrder order) {
        MEOrdersAtPrice ordersAtPrice = priceOrdersAtPrice.get(order.getPrice());
        if (ordersAtPrice == null) {
            return;
        }

        if (order.getPrevOrder() == order) {
            // Single order in this price level
            removeOrdersAtPrice(order.getSide(), order.getPrice());
        } else {
            MEOrder prev = order.getPrevOrder();
            MEOrder next = order.getNextOrder();
            prev.setNextOrder(next);
            next.setPrevOrder(prev);

            if (ordersAtPrice.getFirstMeOrder() == order) {
                ordersAtPrice.setFirstMeOrder(next);
            }
            order.setPrevOrder(null);
            order.setNextOrder(null);
        }

        clientOrders.remove(order);
    }

    private void removeOrdersAtPrice(Side side, long price) {
        MEOrdersAtPrice node = priceOrdersAtPrice.get(price);
        if (node == null) {
            return;
        }

        if (node.getNextEntry() == node) {
            // Single price level
            if (side == Side.BUY) {
                bidsByPrice = null;
            } else {
                asksByPrice = null;
            }
        } else {
            MEOrdersAtPrice prev = node.getPrevEntry();
            MEOrdersAtPrice next = node.getNextEntry();
            prev.setNextEntry(next);
            next.setPrevEntry(prev);

            if (side == Side.BUY && bidsByPrice == node) {
                bidsByPrice = next;
            } else if (side == Side.SELL && asksByPrice == node) {
                asksByPrice = next;
            }
        }
        priceOrdersAtPrice.put(price, null);
    }

    // Insert newNode into the doubly linked list of price levels.
    private void insertPriceLevel(MEOrdersAtPrice best, MEOrdersAtPrice newNode, boolean isBuy) {
        // If the new node is "better" than the current best, insert it as the new best.
        // For buys, a "better" price is higher. For sells, a "better" price is lower.
        if ((isBuy && newNode.getPrice() > best.getPrice()) ||
                (!isBuy && newNode.getPrice() < best.getPrice())) {

            // Insert newNode before 'best' in the ring
            MEOrdersAtPrice prevBest = best.getPrevEntry();
            prevBest.setNextEntry(newNode);
            newNode.setPrevEntry(prevBest);
            newNode.setNextEntry(best);
            best.setPrevEntry(newNode);

            if (isBuy) {
                bidsByPrice = newNode;
            } else {
                asksByPrice = newNode;
            }
            return;
        }

        // For buys: we move forward while newNode's price is <= the next node's price (descending).
        // For sells: we move forward while newNode's price is >= the next node's price (ascending).
        MEOrdersAtPrice current = best;
        while (true) {
            MEOrdersAtPrice next = current.getNextEntry();
            if (next == best || isBuy ? newNode.getPrice() > next.getPrice() : newNode.getPrice() < next.getPrice()) {
                // Insert 'newNode' after 'current' and before 'next'
                newNode.setNextEntry(next);
                newNode.setPrevEntry(current);
                current.setNextEntry(newNode);
                next.setPrevEntry(newNode);
                return;
            }

            current = next; // move forward in the ring
        }
    }

    private long getNextPriority(long price) {
        MEOrdersAtPrice node = priceOrdersAtPrice.get(price);
        return node == null ? 1 : node.getFirstMeOrder().getPrevOrder().getPriority() + 1;
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