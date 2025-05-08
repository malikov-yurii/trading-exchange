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

public class OrderResponseSerDe {
    private static final Logger log = LoggerFactory.getLogger(OrderResponseSerDe.class);

    public static void toFIXMessage(OrderResponse orderResponse, Message msg) {
        msg.clear();
        msg.setString(37, Long.toString(orderResponse.getMarketOrderId())); // OrderID
        msg.setString(17, Long.toString(orderResponse.getSeqNum()));        // ExecID

        char execType;
        char ordStatus;
        char msgType;

        switch (orderResponse.getType()) {
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
                msg.setString(41, Long.toString(orderResponse.getClientOrderId()));        // OrigClOrdID
            }
            case REQUEST_REJECT -> {
                execType = 'z' /* non-relevant */; ordStatus = 'z' /* non-relevant */; msgType = '3';
                msg.setString(58, "Reject New Order Request: Duplicate ClOrdID");
                msg.setString(45, Long.toString(orderResponse.getClientOrderId())); //RefSeqNum
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
        msg.setString(11, Long.toString(orderResponse.getClientOrderId())); // ClOrdID

        msg.setString(55, Long.toString(orderResponse.getTickerId()));      // Symbol
        msg.setChar(54, orderResponse.getSide() == Side.BUY ? '1' : '2');   // Side

        msg.setDouble(151, orderResponse.getLeavesQty());                   // LeavesQty
        msg.setDouble(14, orderResponse.getExecQty());                      // CumQty

        if (msgType == '8') {
            msg.setDouble(6, orderResponse.getExecQty() > 0 ? orderResponse.getPrice() : 0); // AvgPx
            msg.setChar(20, '0');                                              // ExecTransType
            msg.setChar(21, '1');                                              // HandlInst
        }

        msg.setUtcTimeStamp(60, LocalDateTime.now(), true);              // TransactTime

        // Custom tags
        msg.setString(20001, Long.toString(orderResponse.getClientId()));  // clientId
        msg.setString(20002, Long.toString(orderResponse.getSeqNum()));    // seqNum
    }

    public static void toOrderMessage(Message message, OrderResponse orderResponse) throws FieldNotFound {
        orderResponse.setType(getOrderMessageType(message));

//        long clientOrderId = orderMessage.getType() == OrderMessageType.REQUEST_REJECT
//                ? Long.parseLong(message.getString(45))
//                : Long.parseLong(message.getString(ClOrdID.FIELD));
        long clientOrderId = Long.parseLong(message.getString(ClOrdID.FIELD));
        orderResponse.setClientOrderId(clientOrderId);
        orderResponse.setMarketOrderId(message.isSetField(OrderID.FIELD)
                ? Long.parseLong(message.getString(OrderID.FIELD)) : 0);
        orderResponse.setClientId(1); // Replace with custom field extraction if needed
        orderResponse.setTickerId(Long.parseLong(message.getString(Symbol.FIELD)));

        char sideChar = message.getChar(quickfix.field.Side.FIELD);
        orderResponse.setSide(sideChar == quickfix.field.Side.BUY ? Side.BUY : Side.SELL);

        orderResponse.setPrice(message.isSetField(quickfix.field.Price.FIELD)
                ? (long) message.getDouble(quickfix.field.Price.FIELD) : 0);
        orderResponse.setExecQty((long) message.getDouble(CumQty.FIELD));
        orderResponse.setLeavesQty((long) message.getDouble(LeavesQty.FIELD));
    }

    private static OrderResponseType getOrderMessageType(Message message) throws FieldNotFound {
        String msgType = message.getHeader().getString(MsgType.FIELD);
        return switch (msgType) {
            case MsgType.EXECUTION_REPORT -> {
                char execType = message.getChar(ExecType.FIELD);
                yield switch (execType) {
                    case ExecType.NEW -> OrderResponseType.ACCEPTED;
                    case ExecType.FILL, ExecType.PARTIAL_FILL -> OrderResponseType.FILLED;
                    case ExecType.CANCELED -> OrderResponseType.CANCELED;
                    default -> OrderResponseType.INVALID;
                };
            }
            case MsgType.ORDER_CANCEL_REJECT -> OrderResponseType.CANCEL_REJECTED;
            case MsgType.REJECT -> OrderResponseType.REQUEST_REJECT;
            default -> OrderResponseType.INVALID;
        };
    }

}
