package trading.exchange.orderserver;

import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.AeronConsumer;
import trading.common.AeronPublisher;
import trading.common.LFQueue;
import trading.common.Utils;

import java.util.concurrent.atomic.AtomicLong;

public class RequestSequencer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RequestSequencer.class);

    private final LFQueue<OrderRequest> requestQueue;
    private final AtomicLong reqSeqNum = new AtomicLong(1);
    private final AeronPublisher replicationPublisher;
    private final AeronConsumer replicationAckConsumer;

    private final OrderRequest[] ringBuffer = new OrderRequest[1 << 20];

    public RequestSequencer(LFQueue<OrderRequest> clientRequests) {
        this.requestQueue = clientRequests;

        String aeronIp = Utils.env("AERON_IP", "224.0.1.1");

        this.replicationPublisher = new AeronPublisher(aeronUdpChannel(aeronIp,
                Utils.env("REPLICATION_PORT", "40551")),
                Integer.parseInt(Utils.env("REPLICATION_STREAM", "3001")),
                "REPLICATION");

        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> processReplicationAck(buffer, offset, length);
        replicationAckConsumer = new AeronConsumer(aeronIp,
                Utils.env("REPLICATION_ACK_PORT", "40552"),
                Integer.parseInt(Utils.env("REPLICATION_ACK_STREAM", "3002")),
                fragmentHandler, "REPLICATION_ACK");
    }

    public void process(OrderRequest orderRequest) {
        long seq = reqSeqNum.getAndIncrement();
        orderRequest.setSeqNum(seq);
        log.info("Received {}", orderRequest);

        enqueueOrderReq(orderRequest);

        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
        int offset = 0;
        int len = OrderRequestSerDe.serialize(orderRequest, buffer, offset);
        replicationPublisher.publish(buffer, offset, len);
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

    private static String aeronUdpChannel(String aeronIp, String replicationPort) {
        return "aeron:udp?endpoint=" + aeronIp + ":" + replicationPort;
    }

    public void shutdown() {
        replicationPublisher.close();
        replicationAckConsumer.stop();
    }

    @Override
    public void run() {
        log.info("RequestSequencer starting");
        replicationAckConsumer.run();
        log.info("RequestSequencer shutting down");
    }

}
