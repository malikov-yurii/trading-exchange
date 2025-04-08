package trading.exchange.orderserver;

import aeron.ArchiveConsumerAgent;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import aeron.AeronPublisher;
import trading.common.LFQueue;
import trading.common.Utils;

public class ReplicationConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ReplicationConsumer.class);

    private final LFQueue<OrderRequest> clientRequests;

    private final AeronPublisher replicationAckPublisher;
    private final MutableDirectBuffer seqNumBuffer;
    private AgentRunner runner;

    public ReplicationConsumer(LFQueue<OrderRequest> clientRequests) {
        this.clientRequests = clientRequests;

        String aeronIp = Utils.env("AERON_IP", "224.0.1.1");

        this.replicationAckPublisher = new AeronPublisher(
                aeronUdpChannel(aeronIp, Utils.env("REPLICATION_ACK_PORT", "40552")),
                Integer.parseInt(Utils.env("REPLICATION_ACK_STREAM", "3002")),
                "REPLICATION_ACK");

        seqNumBuffer = new ExpandableDirectByteBuffer(8);
    }

    private void processReplicationEvent(DirectBuffer buffer, int offset, int length) {
        OrderRequest orderRequest = OrderRequestSerDe.deserializeClientRequest(buffer, offset, length);
        log.info("Received {} offset {} length {}", orderRequest, offset, length);

        clientRequests.offer(orderRequest);

        seqNumBuffer.putLong(0, orderRequest.getSeqNum());
        replicationAckPublisher.publish(seqNumBuffer, 0, Long.BYTES);
    }

    private static String aeronUdpChannel(String aeronIp, String replicationPort) {
        return "aeron:udp?endpoint=" + aeronIp + ":" + replicationPort;
    }

    public void shutdown() {
        CloseHelper.quietClose(runner);
        replicationAckPublisher.close();
    }

    @Override
    public void run() {
        log.info("ReplicationConsumer starting");

        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> processReplicationEvent(buffer, offset, length);
        int streamId = Integer.parseInt(Utils.env("REPLICATION_STREAM", "3001"));
        ArchiveConsumerAgent.ReplayStrategy replayStrategy = ArchiveConsumerAgent.ReplayStrategy.REPLAY_OLD_AND_SUBSCRIBE;
        final ArchiveConsumerAgent hostAgent =
                new ArchiveConsumerAgent(streamId, fragmentHandler, replayStrategy, "REPLICATION");

        runner = new AgentRunner(new SleepingMillisIdleStrategy(), ArchiveConsumerAgent::errorHandler, null, hostAgent);

        AgentRunner.startOnThread(runner);

        log.info("ReplicationConsumer shutting down");
    }

}
