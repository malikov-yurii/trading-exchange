package trading.api;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;

import java.nio.ByteOrder;

public class MarketUpdateSerDe {

    public static int serializeMarketUpdate(MarketUpdate marketUpdate, ExpandableDirectByteBuffer buffer, int offset) {
        int startOffset = offset;

        buffer.putLong(offset, marketUpdate.getSeqNum());
        offset += Long.BYTES;

        buffer.putByte(offset, (byte) marketUpdate.getType().ordinal());
        offset += 1;

        buffer.putByte(offset, (byte) marketUpdate.getSide().ordinal());
        offset += 1;

        buffer.putLong(offset, marketUpdate.getOrderId());
        offset += Long.BYTES;
        buffer.putLong(offset, marketUpdate.getTickerId());
        offset += Long.BYTES;
        buffer.putLong(offset, marketUpdate.getPrice());
        offset += Long.BYTES;
        buffer.putLong(offset, marketUpdate.getQty());
        offset += Long.BYTES;
        buffer.putLong(offset, marketUpdate.getPriority());
        offset += Long.BYTES;

        return offset - startOffset;
    }

    public static MarketUpdate deserialize(DirectBuffer buffer, int offset) {
        int start = offset;
        long seqNum = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        byte typeOrd = buffer.getByte(start++);
        MarketUpdateType type = MarketUpdateType.values()[typeOrd];

        byte sideOrd = buffer.getByte(start++);
        Side side = Side.values()[sideOrd];

        long orderId = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        long tickerId = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        long price = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        long qty = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        long priority = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        return new MarketUpdate(seqNum, type, orderId, tickerId, side, price, qty, priority);
    }

}
