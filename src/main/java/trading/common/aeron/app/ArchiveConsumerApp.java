package trading.common.aeron.app;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.common.aeron.ArchiveConsumerAgent;
import trading.common.aeron.ArchiveConsumerFragmentHandler;

public class ArchiveConsumerApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveConsumerApp.class);

    public static void main(final String[] args) {
        final var thisHost = System.getenv().get("THISHOST");
        final var archiveHost = System.getenv().get("ARCHIVEHOST");
        final var controlPort = System.getenv().get("CONTROLPORT");
        final var eventPort = System.getenv().get("EVENTSPORT");

        if (archiveHost == null || controlPort == null || thisHost == null || eventPort == null) {
            LOGGER.error("env vars required: THISHOST, ARCHIVEHOST, CONTROLPORT, EVENTSPORT");
        } else {
            final var controlChannelPort = Integer.parseInt(controlPort);
            final var eventChannelPort = Integer.parseInt(eventPort);
            final var barrier = new ShutdownSignalBarrier();
            final var fragmentHandler = new ArchiveConsumerFragmentHandler();
            final ArchiveConsumerAgent hostAgent =
                    new ArchiveConsumerAgent(archiveHost, thisHost, controlChannelPort, eventChannelPort, fragmentHandler,
                            ArchiveConsumerAgent.ReplayStrategy.REPLAY_OLD);
            final var runner =
                    new AgentRunner(new SleepingMillisIdleStrategy(), ArchiveConsumerApp::errorHandler, null, hostAgent);
            AgentRunner.startOnThread(runner);

            barrier.await();

            CloseHelper.quietClose(runner);
        }
    }

    private static void errorHandler(final Throwable throwable) {
        LOGGER.error("agent error {}", throwable.getMessage(), throwable);
    }
}
