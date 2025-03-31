package trading.exchange.orderserver;

import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.AeronConsumer;
import trading.common.AeronPublisher;
import trading.common.LFQueue;
import trading.common.Utils;

public class ReplicationConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ReplicationConsumer.class);

    private final LFQueue<OrderRequest> requestQueue;

    private final AeronConsumer replicationConsumer;
    private final AeronPublisher replicationAckPublisher;
    private final MutableDirectBuffer seqNumBuffer;

    public ReplicationConsumer(LFQueue<OrderRequest> clientRequests) {
        this.requestQueue = clientRequests;

        String aeronIp = Utils.env("AERON_IP", "224.0.1.1");

        this.replicationAckPublisher = new AeronPublisher(
                aeronUdpChannel(aeronIp, Utils.env("REPLICATION_ACK_PORT", "40552")),
                Integer.parseInt(Utils.env("REPLICATION_ACK_STREAM", "3002")),
                "REPLICATION_ACK");

        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> processReplicationEvent(buffer, offset, length);
        replicationConsumer = new AeronConsumer(aeronIp,
                Utils.env("REPLICATION_PORT", "40551"),
                Integer.parseInt(Utils.env("REPLICATION_STREAM", "3001")),
                fragmentHandler, "REPLICATION");

        seqNumBuffer = new ExpandableDirectByteBuffer(8);
    }

    private void processReplicationEvent(DirectBuffer buffer, int offset, int length) {
        OrderRequest orderRequest = OrderRequestSerDe.deserializeClientRequest(buffer, offset, length);
        log.info("Received {}", orderRequest);

        requestQueue.offer(orderRequest);

        seqNumBuffer.putLong(0, orderRequest.getSeqNum());
        replicationAckPublisher.publish(seqNumBuffer, 0, Long.BYTES);
    }

    private static String aeronUdpChannel(String aeronIp, String replicationPort) {
        return "aeron:udp?endpoint=" + aeronIp + ":" + replicationPort;
    }

    public void shutdown() {
        replicationConsumer.stop();
        replicationAckPublisher.close();
    }

    @Override
    public void run() {
        log.info("ReplicationConsumer starting");
        replicationConsumer.run();
        log.info("ReplicationConsumer shutting down");
    }

}
