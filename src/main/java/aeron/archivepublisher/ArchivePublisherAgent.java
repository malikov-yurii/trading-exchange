package aeron.archivepublisher;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingSignalAdapter;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

/**
 * ArchivePublisher is used to:
 * 1) Connect to an existing Aeron + Archive (hosted by ArchiveHostAgent).
 * 2) Start a recording for the given channel/stream.
 * 3) Publish data to that channel every 2s, just like old ArchiveHostAgent did.
 * <p>
 * Launch this *after* you have ArchiveHostAgent running.
 * Combined, they replicate the old single-process approach.
 */
public class ArchivePublisherAgent implements Agent {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArchivePublisherAgent.class);

    // Shared constants
    private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;
    private static final IdleStrategy IDLE_STRATEGY = new SleepingMillisIdleStrategy();
    private static final int STREAM_ID = 100;

    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";

    // Constructor parameters
    private final String thisHost;
    private final int controlChannelPort;
    private final int recordingEventsPort;

    // Internal references
    private Aeron aeron;
    private AeronArchive archive;
    private Publication publication;
    private RecordingSignalAdapter recordingSignalAdapter;
    private RecordingEventsAdapter recordingEventsAdapter;

    private final MutableDirectBuffer mutableDirectBuffer;

    private long nextAppend = Long.MIN_VALUE;
    private long lastSeq = 0;
    private String archiveHost;

    private enum State {
        INIT,
        ARCHIVE_READY,
        PUBLISHING,
        SHUTTING_DOWN
    }

    private State currentState = State.INIT;

    /**
     * Create an ArchivePublisher that will connect to an existing archive
     * at (host, controlChannelPort, recordingEventsPort).
     */
    public ArchivePublisherAgent(final String thisHost, final int controlChannelPort, final int recordingEventsPort) {
        this.thisHost = localHost(thisHost);
        this.controlChannelPort = controlChannelPort;
        this.recordingEventsPort = recordingEventsPort;

        // We just use a direct buffer for the 8-byte messages
        this.mutableDirectBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));
    }

    @Override
    public void onStart() {
        LOGGER.info("ArchivePublisher.onStart()");
        // We create the Aeron instance on startup
        String aeronDir = System.getenv().getOrDefault("AERON_DIR", "/dev/shm/aeron-root");
        LOGGER.info("Aeron directory is {}", aeronDir);
        this.aeron = Aeron.connect(
                new Aeron.Context()
                        .aeronDirectoryName(aeronDir)
                        .errorHandler(this::errorHandler)
                        .idleStrategy(new SleepingMillisIdleStrategy()));

        Agent.super.onStart();
    }

    @Override
    public int doWork() {
        switch (currentState) {
            case INIT -> createArchiveAndStartRecording();
            case ARCHIVE_READY -> appendDataIfNeeded();
            default -> {
            }
        }

        // Let the signal/event adapters process any signals
        if (recordingSignalAdapter != null) {
            recordingSignalAdapter.poll();
        }
        if (recordingEventsAdapter != null) {
            recordingEventsAdapter.poll();
        }

        return 0;
    }

    private void createArchiveAndStartRecording() {
        archiveHost = System.getenv().get("ARCHIVEHOST");
        // Connect to the existing archive:
        final var controlRequestChannel = AERON_UDP_ENDPOINT + archiveHost + ":" + controlChannelPort;
        final var controlResponseChannel = AERON_UDP_ENDPOINT + "localhost" + ":3004";
//        final var controlResponseChannel = AERON_UDP_ENDPOINT + thisHost + ":3004";
        final var recordingEventsChannel = "aeron:udp?control-mode=dynamic|control=" + archiveHost + ":" + recordingEventsPort;

        LOGGER.info("ArchivePublisher connecting to archive at {}", controlRequestChannel);
        String aeronDir = System.getenv().getOrDefault("AERON_DIR", "/dev/shm/aeron-root");
        archive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .aeronDirectoryName(aeronDir)
                .controlRequestChannel(controlRequestChannel)
                .controlResponseChannel(controlResponseChannel)
                .recordingEventsChannel(recordingEventsChannel)
                .idleStrategy(new SleepingMillisIdleStrategy()));

        // Create the publication
        LOGGER.info("Creating publication on aeron:ipc, streamId={}", STREAM_ID);
        publication = aeron.addExclusivePublication("aeron:ipc", STREAM_ID);

        // Start recording that publication
        LOGGER.info("Starting recording on aeron:ipc, streamId={}", STREAM_ID);
        archive.startRecording("aeron:ipc", STREAM_ID, SourceLocation.LOCAL);

        // Wait for the archive to see that session
        final var counters = aeron.countersReader();
        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        while (CountersReader.NULL_COUNTER_ID == counterId) {
            IDLE_STRATEGY.idle();
            counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        }
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        LOGGER.info("Recording is active with recordingId={}", recordingId);

        // Set up a signal adapter
        final var activityListener = new ArchiveActivityListener();
        recordingSignalAdapter = new RecordingSignalAdapter(
                archive.controlSessionId(),
                activityListener,
                activityListener,
                archive.controlResponsePoller().subscription(),
                10
        );

        // Set up a recording-events adapter
        final var recEventsSubscription = aeron.addSubscription(
                archive.context().recordingEventsChannel(),
                archive.context().recordingEventsStreamId()
        );
        final var progressListener = new ArchiveProgressListener();
        recordingEventsAdapter = new RecordingEventsAdapter(progressListener, recEventsSubscription, 10);

        // Move to next state
        currentState = State.ARCHIVE_READY;
        LOGGER.info("ArchivePublisher is now ARCHIVE_READY and will begin appending data");
    }

    private void appendDataIfNeeded() {
        final long nowMs = CLOCK.time();
        if (nowMs >= nextAppend) {
            lastSeq++;
            mutableDirectBuffer.putLong(0, lastSeq);

            // Offer the 8-byte buffer
            long result = publication.offer(mutableDirectBuffer, 0, Long.BYTES);
            if (result > 0) {
                LOGGER.info("Appended seq={}", lastSeq);
            } else {
                LOGGER.warn("Publication.offer() returned {}", result);
            }
            // Next append 2 seconds from now
            nextAppend = nowMs + 2000;
        }
    }

    @Override
    public void onClose() {
        LOGGER.info("ArchivePublisher shutting down");
        currentState = State.SHUTTING_DOWN;
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
        Agent.super.onClose();
    }

    private void errorHandler(Throwable throwable) {
        LOGGER.error("Unexpected error in ArchivePublisher: {}", throwable.getMessage(), throwable);
    }

    @Override
    public String roleName() {
        return "agent-publisher";
    }

    /**
     * Utility method to find local IPv4 address from eth0 (if possible), otherwise fallback
     */
    private String localHost(final String fallback) {
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
        } catch (SocketException e) {
            LOGGER.warn("Unable to resolve localHost from eth0. Using fallback: {}", fallback);
        }
        return fallback;
    }

}