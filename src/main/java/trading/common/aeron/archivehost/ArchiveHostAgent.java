package trading.common.aeron.archivehost;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Objects;

/**
 * ArchiveHostAgent runs the Aeron MediaDriver + Archive in a single process,
 * providing the “archive” component. It does NOT publish data or start recordings
 * on any specific (channel, stream); that is now the job of ArchivePublisher.
 * <p>
 * When launched, this agent:
 * 1) Starts an ArchivingMediaDriver (with the Archive).
 * 2) Creates a client Aeron instance, primarily so we can confirm everything is healthy.
 * 3) Moves to a "RUNNING" state (no further actions needed).
 */
public class ArchiveHostAgent implements Agent {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveHostAgent.class);
    private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;

    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";

    // Provided via constructor
    private final String host;
    private final int controlChannelPort;
    private final int recordingEventsPort;

    // Underlying resources
    private final ArchivingMediaDriver archivingMediaDriver;
    private final Aeron aeron;

    // Simple states for clarity
    private enum State {
        AERON_READY,
        ARCHIVE_RUNNING,
        SHUTTING_DOWN
    }

    private State currentState;

    public ArchiveHostAgent(final String host, final int controlChannelPort, final int recordingEventsPort) {
        this.host = localHost(Objects.requireNonNullElse(host, "127.0.0.1"));
        this.controlChannelPort = controlChannelPort;
        this.recordingEventsPort = recordingEventsPort;

        // Launch ArchivingMediaDriver
        this.archivingMediaDriver = launchMediaDriver(this.host, this.controlChannelPort, this.recordingEventsPort);

        // Create Aeron client
        this.aeron = launchAeron(archivingMediaDriver);

        currentState = State.AERON_READY;

        LOGGER.info("ArchiveHostAgent constructed. Media Driver directory is {}; Archive directory is {}",
                archivingMediaDriver.mediaDriver().aeronDirectoryName(),
                archivingMediaDriver.archive().context().archiveDirectoryName());
    }

    private ArchivingMediaDriver launchMediaDriver(final String host, final int controlChannelPort,
                                                   final int recordingEventsPort) {
        LOGGER.info("Launching ArchivingMediaDriver (Archive + MediaDriver)");
        final String controlChannel = AERON_UDP_ENDPOINT + host + ":" + controlChannelPort;
        final String replicationChannel = AERON_UDP_ENDPOINT + host + ":" + (controlChannelPort + 1);
        final String recordingEventsChannel = "aeron:udp?control-mode=dynamic|control=" + host + ":" + recordingEventsPort;
        boolean deleteOnStart = Boolean.parseBoolean(System.getenv().getOrDefault("DELETE_AERON_DIR", "false"));
        LOGGER.info("Delete Aeron directory on start: {}", deleteOnStart);

        String aeronDir = System.getenv().getOrDefault("AERON_DIR", "/dev/shm/aeron-root");
        LOGGER.info("Aeron directory is {}", aeronDir);

        final var archiveContext = new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .deleteArchiveOnStart(deleteOnStart)
                .errorHandler(this::errorHandler)
                .controlChannel(controlChannel)
                .replicationChannel(replicationChannel)
                .recordingEventsChannel(recordingEventsChannel)
                .idleStrategySupplier(SleepingMillisIdleStrategy::new)
                .threadingMode(ArchiveThreadingMode.SHARED);

        final var mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .spiesSimulateConnection(true)
                .errorHandler(this::errorHandler)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new SleepingMillisIdleStrategy())
                .dirDeleteOnStart(deleteOnStart);

        return ArchivingMediaDriver.launch(mediaDriverContext, archiveContext);
    }

    private Aeron launchAeron(final ArchivingMediaDriver archivingMediaDriver) {
        LOGGER.info("Launching Aeron client connection");
        String aeronDir = archivingMediaDriver.mediaDriver().aeronDirectoryName();
        LOGGER.info("archivingMediaDriver.mediaDriver().aeronDirectoryName() is {}", aeronDir);
        return Aeron.connect(
                new Aeron.Context()
                        .aeronDirectoryName(aeronDir)
                        .errorHandler(this::errorHandler)
                        .idleStrategy(new SleepingMillisIdleStrategy())
        );
    }

    private void errorHandler(final Throwable throwable) {
        LOGGER.error("Unexpected failure {}", throwable.getMessage(), throwable);
    }

    @Override
    public void onStart() {
        LOGGER.info("ArchiveHostAgent.onStart()");
        Agent.super.onStart();
    }

    @Override
    public int doWork() {
        // We just transition to ARCHIVE_RUNNING once everything is set up.
        switch (currentState) {
            case AERON_READY -> {
                currentState = State.ARCHIVE_RUNNING;
                LOGGER.info("Archive is now running.");
            }
            case ARCHIVE_RUNNING -> {
                // No additional work needed here. Archive is up and waiting for external publishers.
            }
            default -> {
            }
        }

        return 0; // No “work” to do per-se
    }

    @Override
    public void onClose() {
        LOGGER.info("ArchiveHostAgent shutting down");
        this.currentState = State.SHUTTING_DOWN;

        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(archivingMediaDriver);

        Agent.super.onClose();
    }

    /**
     * Utility to pick an IPv4 address from eth0 (if available) or else fallback.
     */
    public String localHost(final String fallback) {
        try {
            final Enumeration<NetworkInterface> interfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnumeration.hasMoreElements()) {
                final var networkInterface = interfaceEnumeration.nextElement();
                if (networkInterface.getName().startsWith("eth0")) {
                    final Enumeration<InetAddress> interfaceAddresses = networkInterface.getInetAddresses();
                    while (interfaceAddresses.hasMoreElements()) {
                        final InetAddress address = interfaceAddresses.nextElement();
                        if (address instanceof Inet4Address inet4Address) {
                            LOGGER.info("Detected IPv4 address: {}", inet4Address.getHostAddress());
                            return inet4Address.getHostAddress();
                        }
                    }
                }
            }
        } catch (final SocketException e) {
            LOGGER.info("Failed to get interface addresses: {}", e.getMessage());
        }
        return fallback;
    }

    @Override
    public String roleName() {
        return "agent-host";
    }
}