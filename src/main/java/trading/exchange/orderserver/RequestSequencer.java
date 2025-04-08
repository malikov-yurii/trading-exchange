package trading.exchange.orderserver;

import aeron.AeronConsumer;
import aeron.ArchivePublisher;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.LFQueue;
import trading.common.Utils;

import java.util.concurrent.atomic.AtomicLong;

public class RequestSequencer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RequestSequencer.class);

    private final LFQueue<OrderRequest> requestQueue;
    private final AtomicLong nextSeqNum;
    private final ArchivePublisher archivePublisher;
    private final AeronConsumer replicationAckConsumer;

    private final OrderRequest[] ringBuffer = new OrderRequest[1 << 20];

    public RequestSequencer(LFQueue<OrderRequest> clientRequests, long seqNum) {
        nextSeqNum = new AtomicLong(seqNum + 1);
        this.requestQueue = clientRequests;

        String aeronIp = Utils.env("AERON_IP", "224.0.1.1");

        int replicationStream = Integer.parseInt(Utils.env("REPLICATION_STREAM", "3001"));
        this.archivePublisher = new ArchivePublisher(replicationStream, "REPLICATION", false);

        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> processReplicationAck(buffer, offset, length);
        replicationAckConsumer = new AeronConsumer(aeronIp,
                Utils.env("REPLICATION_ACK_PORT", "40552"),
                Integer.parseInt(Utils.env("REPLICATION_ACK_STREAM", "3002")),
                fragmentHandler, "REPLICATION_ACK");
    }

    public void process(OrderRequest orderRequest) {
        long seq = nextSeqNum.getAndIncrement();
        orderRequest.setSeqNum(seq);
        log.info("Received {}", orderRequest);

        enqueueOrderReq(orderRequest);

        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
        int offset = 0;
        int length = OrderRequestSerDe.serialize(orderRequest, buffer, offset);
        archivePublisher.publish(buffer, offset, length);
    }

    private void enqueueOrderReq(OrderRequest orderRequest) {
        int ind = (int) (orderRequest.getSeqNum() % ringBuffer.length);
        while (ringBuffer[ind] != null) {
            final int millis = 10;
            log.warn("RingBuffer is full. Sleeping {}ms", millis);
            try {

                Thread.sleep(millis);
            } catch (InterruptedException e) {
                // Ignore
            }
            return;
        }
        ringBuffer[ind] = orderRequest;
    }

    private void processReplicationAck(DirectBuffer buffer, int offset, int length) {
        long ackedReqSeqNum = buffer.getLong(offset);

        OrderRequest orderRequest = dequeueOrderReq(ackedReqSeqNum);

        if (orderRequest == null) {
            log.info("Received Dup Ack for msgSeqNum={}", ackedReqSeqNum);
            // Dup Ack
            return;
        }
        log.info("Received Ack for msgSeqNum={} OrderRequest: {}", ackedReqSeqNum, orderRequest);
        requestQueue.offer(orderRequest);
    }

    private OrderRequest dequeueOrderReq(long ackedReqSeqNum) {
        int ind = (int) (ackedReqSeqNum % ringBuffer.length);
        OrderRequest orderRequest = ringBuffer[ind];
        ringBuffer[ind] = null;
        return orderRequest;
    }

    public void shutdown() {
        archivePublisher.close();
        replicationAckConsumer.stop();
    }

    @Override
    public void run() {
        log.info("RequestSequencer starting");
        replicationAckConsumer.run();
        log.info("RequestSequencer shutting down");
    }

}
