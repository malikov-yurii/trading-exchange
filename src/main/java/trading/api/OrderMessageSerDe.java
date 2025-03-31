package trading.api;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderMessageSerDe {

    private static final Logger log = LoggerFactory.getLogger(OrderMessageSerDe.class);

    /**
     * OrderMessage format from server (little-endian):
     *  - 8 bytes: seqNum
     *  - 1 byte: type ordinal
     *  - 1 byte: side ordinal
     *  - 8 bytes: clientId
     *  - 8 bytes: tickerId
     *  - 8 bytes: clientOrderId
     *  - 8 bytes: marketOrderId
     *  - 8 bytes: price
     *  - 8 bytes: execQty
     *  - 8 bytes: leavesQty
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
}
