package aeron.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
//            final var controlChannelPort = Integer.parseInt(controlPort);
//            final var eventChannelPort = Integer.parseInt(eventPort);
//            final var barrier = new ShutdownSignalBarrier();
//            final var fragmentHandler = new ArchiveConsumerFragmentHandler();
//            final ArchiveConsumerAgent hostAgent =
//                    new ArchiveConsumerAgent(fragmentHandler,
//                            ArchiveConsumerAgent.ReplayStrategy.REPLAY_OLD, "name");
//            final var runner =
//                    new AgentRunner(new SleepingMillisIdleStrategy(), ArchiveConsumerAgent::errorHandler, null, hostAgent);
//            AgentRunner.startOnThread(runner);
//
//            barrier.await();
//
//            CloseHelper.quietClose(runner);
        }
    }
}
