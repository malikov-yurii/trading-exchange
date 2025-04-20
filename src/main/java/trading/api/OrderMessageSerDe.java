package trading.api;

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

    public static void toFIXMessage(OrderMessage orderMessage, Message msg) {
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
                execType = '9'; ordStatus = '8'; msgType = '9';
                msg.setString(434, "1");        // CxlRejResponseTo 1: Cancel 2: Amend
                msg.setString(41, Long.toString(orderMessage.getClientOrderId()));        // OrigClOrdID
            }
            case REQUEST_REJECT -> {
                execType = 'z' /* non-relevant */; ordStatus = 'z' /* non-relevant */; msgType = '3';
                msg.setString(58, "Reject New Order Request: Duplicate ClOrdID");
                msg.setString(45, Long.toString(orderMessage.getClientOrderId())); //RefSeqNum
            }
            default -> {
                execType = '8'; ordStatus = '8'; msgType = '8';
            }
        }

        msg.getHeader().setChar(35, msgType);


        if (msgType != '3') {
            msg.setChar(150, execType); // ExecType
            msg.setChar(39, ordStatus); // OrdStatus
        }
        msg.setString(11, Long.toString(orderMessage.getClientOrderId())); // ClOrdID

        msg.setString(55, Long.toString(orderMessage.getTickerId()));      // Symbol
        msg.setChar(54, orderMessage.getSide() == Side.BUY ? '1' : '2');   // Side

        msg.setDouble(151, orderMessage.getLeavesQty());                   // LeavesQty
        msg.setDouble(14, orderMessage.getExecQty());                      // CumQty

        if (msgType == '8') {
            msg.setDouble(6, orderMessage.getExecQty() > 0 ? orderMessage.getPrice() : 0); // AvgPx
            msg.setChar(20, '0');                                              // ExecTransType
            msg.setChar(21, '1');                                              // HandlInst
        }

        msg.setUtcTimeStamp(60, LocalDateTime.now(), true);              // TransactTime

        // Custom tags
        msg.setString(20001, Long.toString(orderMessage.getClientId()));  // clientId
        msg.setString(20002, Long.toString(orderMessage.getSeqNum()));    // seqNum
    }

    public static void toOrderMessage(Message message, OrderMessage orderMessage) throws FieldNotFound {
        orderMessage.setType(getOrderMessageType(message));

//        long clientOrderId = orderMessage.getType() == OrderMessageType.REQUEST_REJECT
//                ? Long.parseLong(message.getString(45))
//                : Long.parseLong(message.getString(ClOrdID.FIELD));
        long clientOrderId = Long.parseLong(message.getString(ClOrdID.FIELD));
        orderMessage.setClientOrderId(clientOrderId);
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

    private static OrderMessageType getOrderMessageType(Message message) throws FieldNotFound {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        return switch (msgType) {
            case MsgType.EXECUTION_REPORT -> {
                char execType = message.getChar(ExecType.FIELD);
                yield switch (execType) {
                    case ExecType.NEW -> OrderMessageType.ACCEPTED;
                    case ExecType.FILL, ExecType.PARTIAL_FILL -> OrderMessageType.FILLED;
                    case ExecType.CANCELED -> OrderMessageType.CANCELED;
                    default -> OrderMessageType.INVALID;
                };
            }
            case MsgType.ORDER_CANCEL_REJECT -> OrderMessageType.CANCEL_REJECTED;
            case MsgType.REJECT -> OrderMessageType.REQUEST_REJECT;
            default -> OrderMessageType.INVALID;
        };
    }

}
