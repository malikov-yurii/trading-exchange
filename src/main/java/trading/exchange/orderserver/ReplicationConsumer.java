package trading.exchange.orderserver;

import aeron.AeronPublisher;
import aeron.AeronUtils;
import aeron.ArchiveConsumerAgent;
import io.aeron.logbuffer.FragmentHandler;
import lombok.Getter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.AsyncLogger;
import trading.common.LFQueue;
import trading.common.Utils;
import trading.exchange.AppState;

public class ReplicationConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ReplicationConsumer.class);

    private final LFQueue<OrderRequest> clientRequests;

    private final AeronPublisher replicationAckPublisher;
    private final MutableDirectBuffer seqNumBuffer;
    private final AsyncLogger asyncLogger;

    private AgentRunner runner;
    private final AppState appState;

    @Getter
    private volatile long lastSeqNum = 0;

    public ReplicationConsumer(LFQueue<OrderRequest> clientRequests, AppState appState, AsyncLogger asyncLogger) {
        this.clientRequests = clientRequests;
        this.appState = appState;
        this.asyncLogger = asyncLogger;

        String aeronIp = Utils.env("AERON_IP", "224.0.1.1");

        this.replicationAckPublisher = new AeronPublisher(
                AeronUtils.aeronUdpChannel(aeronIp, Utils.env("REPLICATION_ACK_PORT", "40552")),
                Integer.parseInt(Utils.env("REPLICATION_ACK_STREAM", "3002")),
                "REPLICATION_ACK");

        seqNumBuffer = new ExpandableDirectByteBuffer(8);
    }

    private void processReplicationEvent(DirectBuffer buffer, int offset, int length) {
        OrderRequest orderRequest = OrderRequestSerDe.deserializeClientRequest(buffer, offset, length);
        asyncLogger.log("REPLICATION", Level.INFO, "|11=%s| Received %s offset %s length %s",
                orderRequest.getOrderId(), orderRequest, offset, length);

        lastSeqNum = orderRequest.getSeqNum();

        clientRequests.offer(orderRequest);

        seqNumBuffer.putLong(0, orderRequest.getSeqNum());
        replicationAckPublisher.publish(seqNumBuffer, 0, Long.BYTES);
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
        final ArchiveConsumerAgent archiveConsumer =
                new ArchiveConsumerAgent(streamId, fragmentHandler, replayStrategy, "REPLICATION");

        archiveConsumer.onReplayOldFinish(appState::setRecovered);

        runner = new AgentRunner(new SleepingMillisIdleStrategy(), ArchiveConsumerAgent::errorHandler, null, archiveConsumer);

        AgentRunner.startOnThread(runner);

        log.info("ReplicationConsumer shutting down");
    }

}
