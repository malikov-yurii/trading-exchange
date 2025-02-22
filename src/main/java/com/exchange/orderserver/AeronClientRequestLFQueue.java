package com.exchange.orderserver;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

// Pseudo interface for a lock-free queue of ClientRequest
public class AeronClientRequestLFQueue implements LFQueue<ClientRequest> {

    private final Aeron aeron;
    private final Publication publication;
    private final Subscription subscription;
    private final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

    // For demonstration, we manage driver internally.
    // In production, you might have a shared or external MediaDriver.
    private final MediaDriver driver;

    // Buffers for serialization/deserialization (thread-local or reused to reduce GC).
    private final UnsafeBuffer sendBuffer;
    private final UnsafeBuffer recvBuffer;

    // Simple constructor that starts an embedded MediaDriver,
    // creates an Aeron instance, and sets up a single channel/stream.
    public AeronClientRequestLFQueue(String channel, int streamId) {
        // Start embedded MediaDriver
        driver = MediaDriver.launchEmbedded();
        Aeron.Context ctx = new Aeron.Context();
        ctx.aeronDirectoryName(driver.aeronDirectoryName());

        aeron = Aeron.connect(ctx);
        publication = aeron.addPublication(channel, streamId);
        subscription = aeron.addSubscription(channel, streamId);

        // Example: Use direct buffers to reduce GC
        ByteBuffer directSendBuffer = ByteBuffer.allocateDirect(1024);
        ByteBuffer directRecvBuffer = ByteBuffer.allocateDirect(1024);
        sendBuffer = new UnsafeBuffer(directSendBuffer);
        recvBuffer = new UnsafeBuffer(directRecvBuffer);
    }

    @Override
    public void offer(ClientRequest request) {
        // Serialize request into sendBuffer
        int length = serializeClientRequest(request, sendBuffer, 0);

        long result;
        do {
            result = publication.offer(sendBuffer, 0, length);
            if (result > 0) {
                return;
            } else if (result == Publication.BACK_PRESSURED || result == Publication.NOT_CONNECTED) {
                // Busy spin or yield
                idleStrategy.idle();
            } else {
                // Other conditions (CLOSED, MAX_POSITION)
                System.err.println("Publication error, result=" + result);
            }
        } while (true);
    }

    @Override
    public ClientRequest poll() {
        // Attempt to poll from subscription
        final ClientRequest[] holder = new ClientRequest[1]; // hacky closure capture

        subscription.poll((buffer, offset, length, header) -> {
            ClientRequest request = deserializeClientRequest(buffer, offset, length);
            holder[0] = request;
        }, 1);

        return holder[0];
    }

    // Example serialization: Very naive.
    // Writes: seqNum (8 bytes), type (1 byte), side (1 byte), clientId/tickerId/orderId/price/qty (8 bytes each).
    // You can replace with SBE or your own scheme.
    private int serializeClientRequest(ClientRequest req, UnsafeBuffer buffer, int offset) {
        int pos = offset;
        buffer.putLong(pos, req.getSeqNum());
        pos += 8;

        // MEClientRequest
        buffer.putByte(pos, (byte) req.getType().ordinal());
        pos += 1;
        buffer.putByte(pos, (byte) req.getSide().ordinal());
        pos += 1;
        buffer.putLong(pos, req.getClientId());
        pos += 8;
        buffer.putLong(pos, req.getTickerId());
        pos += 8;
        buffer.putLong(pos, req.getOrderId());
        pos += 8;
        buffer.putLong(pos, req.getPrice());
        pos += 8;
        buffer.putLong(pos, req.getQty());
        pos += 8;

        return pos - offset;
    }

    private ClientRequest deserializeClientRequest(DirectBuffer buffer, int offset, int length) {
        ClientRequest req = new ClientRequest();

        int pos = offset;
        long seq = buffer.getLong(pos);
        req.setSeqNum(seq);
        pos += 8;

        ClientRequestType[] crtValues = ClientRequestType.values();
        Side[] sideValues = Side.values();
        byte typeOrd = buffer.getByte(pos++);
        byte sideOrd = buffer.getByte(pos++);

        req.setType((typeOrd >= 0 && typeOrd < crtValues.length) ? crtValues[typeOrd] : ClientRequestType.INVALID);
        req.setSide((sideOrd >= 0 && sideOrd < sideValues.length) ? sideValues[sideOrd] : Side.INVALID);

        req.setClientId(buffer.getLong(pos));   pos += 8;
        req.setTickerId(buffer.getLong(pos));   pos += 8;
        req.setOrderId(buffer.getLong(pos));    pos += 8;
        req.setPrice(buffer.getLong(pos));      pos += 8;
        req.setQty(buffer.getLong(pos));        pos += 8;

        return req;
    }

    public void close() {
        subscription.close();
        publication.close();
        aeron.close();
        driver.close();
    }

}