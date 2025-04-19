package trading.api;

import io.netty.buffer.ByteBuf;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.ExecType;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrderID;
import quickfix.field.Symbol;

import java.time.LocalDateTime;

public class OrderMessageSerDe {

    private static final Logger log = LoggerFactory.getLogger(OrderMessageSerDe.class);

    public static int serialize(OrderMessage orderMessage, ExpandableDirectByteBuffer buffer, int offset) {
        int startOffset = offset;

        buffer.putLong(offset, orderMessage.getSeqNum());
        offset += Long.BYTES;

        buffer.putByte(offset, (byte) orderMessage.getType().ordinal());
        offset += Byte.BYTES;

        buffer.putByte(offset, (byte) orderMessage.getSide().ordinal());
        offset += Byte.BYTES;

        buffer.putLong(offset, orderMessage.getClientId());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getTickerId());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getClientOrderId());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getMarketOrderId());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getPrice());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getExecQty());
        offset += Long.BYTES;

        buffer.putLong(offset, orderMessage.getLeavesQty());
        offset += Long.BYTES;

        return offset - startOffset;
    }

    /**
     * OrderMessage format from server (little-endian):
     * - 8 bytes: seqNum
     * - 1 byte: type ordinal
     * - 1 byte: side ordinal
     * - 8 bytes: clientId
     * - 8 bytes: tickerId
     * - 8 bytes: clientOrderId
     * - 8 bytes: marketOrderId
     * - 8 bytes: price
     * - 8 bytes: execQty
     * - 8 bytes: leavesQty
     */
    public static OrderMessage parseOrderMessage(ByteBuf buf) {
        try {
            int offset = buf.readerIndex();

            long seqNum = buf.getLong(offset);
            offset += Long.BYTES;
            byte typeOrd = buf.getByte(offset++);
            OrderMessageType type = fromTypeOrd(typeOrd);
            byte sideOrd = buf.getByte(offset++);
            Side side = fromSideOrd(sideOrd);

            long clientId = buf.getLong(offset);
            offset += Long.BYTES;
            long tickerId = buf.getLong(offset);
            offset += Long.BYTES;
            long clientOrderId = buf.getLong(offset);
            offset += Long.BYTES;
            long marketOrderId = buf.getLong(offset);
            offset += Long.BYTES;
            long price = buf.getLong(offset);
            offset += Long.BYTES;
            long execQty = buf.getLong(offset);
            offset += Long.BYTES;
            long leavesQty = buf.getLong(offset);
            offset += Long.BYTES;

            OrderMessage resp = new OrderMessage();
            resp.setSeqNum(seqNum);
            resp.setType(type);
            resp.setSide(side);
            resp.setClientId(clientId);
            resp.setTickerId(tickerId);
            resp.setClientOrderId(clientOrderId);
            resp.setMarketOrderId(marketOrderId);
            resp.setPrice(price);
            resp.setExecQty(execQty);
            resp.setLeavesQty(leavesQty);

            return resp;
        } catch (Exception e) {
            log.error("Error parsing OrderMessage", e);
            // Optionally rethrow or return a partial object
            throw e;
        }
    }

    private static OrderMessageType fromTypeOrd(byte ord) {
        OrderMessageType[] values = OrderMessageType.values();
        if (ord < 0 || ord >= values.length) {
            return OrderMessageType.INVALID;
        }
        return values[ord];
    }

    private static Side fromSideOrd(byte ord) {
        Side[] sides = Side.values();
        if (ord < 0 || ord >= sides.length) {
            return Side.INVALID;
        }
        return sides[ord];
    }

    public static void toFIXMessage(OrderMessage orderMessage, Message msg) throws Exception {
        msg.clear();
        msg.setString(37, Long.toString(orderMessage.getMarketOrderId())); // OrderID
        msg.setString(17, Long.toString(orderMessage.getSeqNum()));        // ExecID

        char execType;
        char ordStatus;
        char msgType;

        switch (orderMessage.getType()) {
            case ACCEPTED -> {
                execType = '0'; ordStatus = '0'; msgType = '8';
            }
            case CANCELED -> {
                execType = '4'; ordStatus = '4'; msgType = '8';
            }
            case FILLED -> {
                execType = '2'; ordStatus = '2'; msgType = '8';
            }
            case CANCEL_REJECTED -> {
                execType = '8'; ordStatus = '8'; msgType = '9';
            }
            case REJECTED -> {
                execType = '8'; ordStatus = '8'; msgType = '8';
            }
            default -> {
                execType = '8'; ordStatus = '8'; msgType = '8';
            }
        }

        msg.getHeader().setChar(35, msgType);


        msg.setChar(150, execType); // ExecType
        msg.setChar(39, ordStatus); // OrdStatus

        msg.setString(11, Long.toString(orderMessage.getClientOrderId())); // ClOrdID
        msg.setString(55, Long.toString(orderMessage.getTickerId()));      // Symbol
        msg.setChar(54, orderMessage.getSide() == Side.BUY ? '1' : '2');   // Side

        msg.setDouble(151, orderMessage.getLeavesQty());                   // LeavesQty
        msg.setDouble(14, orderMessage.getExecQty());                      // CumQty
        msg.setDouble(6, orderMessage.getExecQty() > 0 ? orderMessage.getPrice() : 0); // AvgPx

        msg.setUtcTimeStamp(60, LocalDateTime.now(), true);              // TransactTime
        msg.setChar(20, '0');                                              // ExecTransType
        msg.setChar(21, '1');                                              // HandlInst

        // Custom tags
        msg.setString(20001, Long.toString(orderMessage.getClientId()));  // clientId
        msg.setString(20002, Long.toString(orderMessage.getSeqNum()));    // seqNum
    }

    public static void toOrderMessage(Message message, OrderMessage orderMessage) throws FieldNotFound {
        String msgType = message.getHeader().getString(MsgType.FIELD);

        ExecType execType = new ExecType(message.getChar(ExecType.FIELD));
        OrderMessageType type;

        switch (execType.getValue()) {
            case ExecType.NEW:
                type = OrderMessageType.ACCEPTED;
                break;
            case ExecType.FILL:
            case ExecType.PARTIAL_FILL:
                type = OrderMessageType.FILLED;
                break;
            case ExecType.CANCELED:
                type = OrderMessageType.CANCELED;
                break;
            case ExecType.REJECTED:
                if (MsgType.EXECUTION_REPORT.equals(msgType)) {
                    type = OrderMessageType.REJECTED;
                } else if (MsgType.ORDER_CANCEL_REJECT.equals(msgType)) {
                    type = OrderMessageType.CANCEL_REJECTED;
                } else {
                    type = OrderMessageType.INVALID;
                }
                break;
            default:
                type = OrderMessageType.INVALID;
        }

        orderMessage.setType(type);
        orderMessage.setClientOrderId(Long.parseLong(message.getString(ClOrdID.FIELD)));
        orderMessage.setMarketOrderId(message.isSetField(OrderID.FIELD)
                ? Long.parseLong(message.getString(OrderID.FIELD)) : 0);
        orderMessage.setClientId(1); // Replace with custom field extraction if needed
        orderMessage.setTickerId(Long.parseLong(message.getString(Symbol.FIELD)));

        char sideChar = message.getChar(quickfix.field.Side.FIELD);
        orderMessage.setSide(sideChar == quickfix.field.Side.BUY ? Side.BUY : Side.SELL);

        orderMessage.setPrice(message.isSetField(quickfix.field.Price.FIELD)
                ? (long) message.getDouble(quickfix.field.Price.FIELD) : 0);
        orderMessage.setExecQty((long) message.getDouble(CumQty.FIELD));
        orderMessage.setLeavesQty((long) message.getDouble(LeavesQty.FIELD));
    }

}
