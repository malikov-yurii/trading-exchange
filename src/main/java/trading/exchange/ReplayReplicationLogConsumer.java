package trading.exchange;

import aeron.ArchiveConsumerAgent;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.LFQueue;
import trading.common.Utils;

public class ReplayReplicationLogConsumer {
    private static final Logger log = LoggerFactory.getLogger(ReplayReplicationLogConsumer.class);

    private final LFQueue<OrderRequest> clientRequests;

    public ReplayReplicationLogConsumer(LFQueue<OrderRequest> clientRequests) {
        this.clientRequests = clientRequests;
    }

    public void run() {
        int streamId = Integer.parseInt(Utils.env("REPLICATION_STREAM", "3001"));
        log.info("Starting ReplayReplicationLogConsumer with streamId {}", streamId);
        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> processReplicationEvent(buffer, offset, length);
        final ArchiveConsumerAgent hostAgent = new ArchiveConsumerAgent(streamId, fragmentHandler,
                ArchiveConsumerAgent.ReplayStrategy.REPLAY_OLD, "REPLICATION-REPLAY");

        AgentRunner runner = new AgentRunner(new SleepingMillisIdleStrategy(), ArchiveConsumerAgent::errorHandler, null, hostAgent);

        Thread thread = AgentRunner.startOnThread(runner);
        try {
            log.info("ReplayReplicationLogConsumer started");
            thread.join();
        } catch (InterruptedException e) {
            log.error("Thread interrupted", e);
            throw new RuntimeException(e);
        }
        log.info("ReplayReplicationLogConsumer finished");
    }

    private void processReplicationEvent(DirectBuffer buffer, int offset, int length) {
        OrderRequest orderRequest = OrderRequestSerDe.deserializeClientRequest(buffer, offset, length);
        log.info("Received {} offset {} lenght {}", orderRequest, offset, length);

        clientRequests.offer(orderRequest);
    }

}
