package trading.api;

import org.agrona.AbstractMutableDirectBuffer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderRequestSerDe {

    private static final Logger log = LoggerFactory.getLogger(OrderRequestSerDe.class);

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
        OrderRequest req = new OrderRequest();

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

}
