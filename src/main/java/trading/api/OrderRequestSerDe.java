package trading.api;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.UnsupportedMessageType;
import quickfix.field.MsgType;
import quickfix.fix42.NewOrderSingle;

import java.time.LocalDateTime;

import static quickfix.field.HandlInst.AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION;
import static quickfix.field.OrdType.LIMIT;



public class OrderRequestSerDe {

    private static final Logger log = LoggerFactory.getLogger(OrderRequestSerDe.class);
    private static final ThreadLocal<OrderRequest> orderRequestThreadLocal = ThreadLocal.withInitial(OrderRequest::new);

    public static int serialize(OrderRequest orderRequest, MutableDirectBuffer buffer, int offset) {
        int startOffset = offset;

        long clientId = orderRequest.getClientId();
        int side = orderRequest.getSide().ordinal();
        long price = orderRequest.getPrice();
        long qty = orderRequest.getQty();
        int tickerId = (int) orderRequest.getTickerId();
        byte requestType = orderRequest.getType().getValue();

        buffer.putByte(offset, requestType);
        offset += Byte.BYTES;

        buffer.putLong(offset, orderRequest.getSeqNum());
        offset += Long.BYTES;

        buffer.putByte(offset, (byte) side);
        offset += Byte.BYTES;

        buffer.putLong(offset, clientId);
        offset += Long.BYTES;

        buffer.putLong(offset, tickerId); // tickerId
        offset += Long.BYTES;

        buffer.putLong(offset, orderRequest.getOrderId()); // orderId
        offset += Long.BYTES;

        buffer.putLong(offset, price);
        offset += Long.BYTES;

        buffer.putLong(offset, qty);
        offset += Long.BYTES;

        return offset - startOffset;
    }

    public static OrderRequest deserializeClientRequest(final DirectBuffer data, int offset, int length) {
        OrderRequest req = orderRequestThreadLocal.get();

        // Read request type first (1 byte)
        byte requestType = data.getByte(offset);
        offset += Byte.BYTES;
        OrderRequestType type = OrderRequestType.fromValue(requestType);
        req.setType(type);
        if (log.isDebugEnabled()) {
            log.debug("deserializeClientRequest. requestType={}, type={}", requestType, type);
        }

        // Read sequence number (8 bytes)
        long seq = data.getLong(offset);
        offset += Long.BYTES;
        req.setSeqNum(seq);

        // Read side (1 byte)
        byte sideOrd = data.getByte(offset);
        offset += Byte.BYTES;
        req.setSide(Side.values()[sideOrd]);

        // Read clientId (8 bytes)
        req.setClientId(data.getLong(offset));
        offset += Long.BYTES;

        // Read tickerId (8 bytes)
        req.setTickerId(data.getLong(offset));
        offset += Long.BYTES;

        // Read orderId (8 bytes)
        req.setOrderId(data.getLong(offset));
        offset += Long.BYTES;

        // Read price (8 bytes)
        req.setPrice(data.getLong(offset));
        offset += Long.BYTES;

        // Read qty (8 bytes)
        req.setQty(data.getLong(offset));
        offset += Long.BYTES;

        return req;
    }

    public static void toFIXMessage(OrderRequest request, Message fixMessage) {
        fixMessage.getHeader().clear();
        fixMessage.clear();
        fixMessage.getTrailer().clear();

        final String orderId = String.valueOf(request.getOrderId());

        char side = request.getSide() == Side.BUY ? quickfix.field.Side.BUY : quickfix.field.Side.SELL;

        if (request.getType() == OrderRequestType.NEW) {
            fixMessage.getHeader().setString(35, "D"); // MsgType = NewOrderSingle
            fixMessage.setChar(40, LIMIT);             // OrdType
            fixMessage.setDouble(44, request.getPrice()); // Price
            fixMessage.setChar(21, AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION); // HandlInst
        } else if (request.getType() == OrderRequestType.CANCEL) {
            fixMessage.getHeader().setString(35, "F"); // MsgType = OrderCancelRequest
            fixMessage.setString(41, orderId);         // OrigClOrdID
        } else {
            throw new IllegalArgumentException("Unsupported order request type: " + request.getType());
        }

        fixMessage.setInt(20001, (int) request.getClientId());  // Custom tag: clientId
        fixMessage.setString(55, String.valueOf(request.getTickerId())); // Symbol
        fixMessage.setString(11, orderId);                      // ClOrdID
        fixMessage.setChar(54, side);                           // Side
        fixMessage.setDouble(38, request.getQty());             // OrderQty
        fixMessage.setUtcTimeStamp(60, LocalDateTime.now(), true);              // TransactTime
    }

    public static OrderRequest getOrderRequest(Message message) throws FieldNotFound, UnsupportedMessageType {
        OrderRequest request = orderRequestThreadLocal.get();
        String msgType = message.getHeader().getString(MsgType.FIELD);

        if (msgType.equals(MsgType.ORDER_SINGLE)) { // New Order
            NewOrderSingle order = (NewOrderSingle) message;

            long clientId = getCustomLongField(message, 20001, -1); // Optional tag for client ID
            long seqNum = getCustomLongField(message, 20002, -1);  // Optional tag for seqNum
            long orderId = Long.parseLong(order.getClOrdID().getValue());
            long tickerId = Long.parseLong(order.getSymbol().getValue());
            char sideChar = order.getSide().getValue();
            long qty = (long) order.getOrderQty().getValue();
            long price = (long) order.getPrice().getValue();

            request.set(
                    seqNum,
                    OrderRequestType.NEW,
                    clientId,
                    tickerId,
                    orderId,
                    sideChar == quickfix.field.Side.BUY ? Side.BUY : Side.SELL,
                    price,
                    qty
            );
        } else if (msgType.equals(MsgType.ORDER_CANCEL_REQUEST)) {
            quickfix.fix42.OrderCancelRequest cancel = (quickfix.fix42.OrderCancelRequest) message;

            long clientId = getCustomLongField(message, 20001, -1);
            long seqNum = getCustomLongField(message, 20002, -1);
            long orderId = Long.parseLong(cancel.getClOrdID().getValue());
            long tickerId = Long.parseLong(cancel.getSymbol().getValue());
            char sideChar = cancel.getSide().getValue();

            request.set(
                    seqNum,
                    OrderRequestType.CANCEL,
                    clientId,
                    tickerId,
                    orderId,
                    sideChar == quickfix.field.Side.BUY ? Side.BUY : Side.SELL,
                    0,
                    0
            );
        } else {
            throw new UnsupportedMessageType();
        }
        return request;
    }

    public static long getCustomLongField(Message message, int tag, long defaultValue) {
        try {
            if (message.isSetField(tag)) {
                return Long.parseLong(message.getString(tag));
            }
        } catch (Exception e) {
            log.warn("Failed to parse custom field {}: {}", tag, e.getMessage());
        }
        return defaultValue;
    }
}
