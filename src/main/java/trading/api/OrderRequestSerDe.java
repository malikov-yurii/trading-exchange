package trading.api;

import org.agrona.ExpandableDirectByteBuffer;

public class OrderRequestSerDe {

    public static byte[] serialize(OrderRequest orderRequest) {
        long clientId = orderRequest.getClientId();
        int side = orderRequest.getSide().ordinal();
        long price = orderRequest.getPrice();
        long qty = orderRequest.getQty();
        int tickerId = (int) orderRequest.getTickerId();
        byte requestType = orderRequest.getType().getValue();

        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128); // Ensure enough capacity
        int offset = 0;

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

        byte[] data = new byte[offset];

        buffer.getBytes(0, data);
        return data;
    }

}
