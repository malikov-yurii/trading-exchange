package aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ControlEventListener;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingEventsListener;
import io.aeron.archive.client.RecordingSignalAdapter;
import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static aeron.AeronUtils.sleep;

/**
 * ArchivePublisher is used to:
 * 1) Connect to an existing Aeron + Archive (hosted by ArchiveHostAgent).
 * 2) Start a recording for the given channel/stream.
 * Launch this *after* you have ArchiveHostAgent running.
 */
public class ArchivePublisher {
    public static final String CHANNEL = "aeron:ipc";
    private static final Logger log = LoggerFactory.getLogger(ArchivePublisher.class);

    private static final IdleStrategy IDLE_STRATEGY = new SleepingMillisIdleStrategy();

    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";

    private final int controlChannelPort;
    private final int recordingEventsPort;
    private final int streamId;
    private final String archiveHost;
    private final String aeronDir;
    private final String name;

    private Aeron aeron;
    private AeronArchive archive;
    private Publication publication;

    private ScheduledExecutorService scheduler;
    private RecordingSignalAdapter recordingSignalAdapter;
    private RecordingEventsAdapter recordingEventsAdapter;

//    private final MutableDirectBuffer mutableDirectBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));

    private final boolean requireConnectedConsumer;

    private State currentState = State.INIT;

    /**
     * ArchivePublisher connect to an existing archive at (host, controlChannelPort, recordingEventsPort).
     */
    public ArchivePublisher(int streamId, String name, boolean requireConnectedConsumer) {

        archiveHost = System.getenv().get("ARCHIVEHOST");
        String controlport = System.getenv().get("CONTROLPORT");
        String eventsport = System.getenv().get("EVENTSPORT");

        if (ObjectUtils.anyNull(archiveHost, controlport, eventsport)) {
            log.error("requires 3 env vars: ARCHIVEHOST ({}), CONTROLPORT ({}), EVENTSPORT ({})",
                    archiveHost, controlport, eventsport);
            throw new IllegalArgumentException("ArchivePublisher requires 3 env vars: ARCHIVEHOST, CONTROLPORT, EVENTSPORT");
        }

        this.controlChannelPort = Integer.parseInt(controlport);
        this.recordingEventsPort = Integer.parseInt(eventsport);
        this.streamId = streamId;
        this.name = name;
        this.requireConnectedConsumer = requireConnectedConsumer;
        aeronDir = AeronUtils.getAeronDirRemote();
    }

    public void start() {
        log.info("start. Aeron directory is {}", aeronDir);
        this.aeron = Aeron.connect(
                new Aeron.Context()
                        .aeronDirectoryName(aeronDir)
                        .errorHandler(this::errorHandler)
                        .idleStrategy(new SleepingMillisIdleStrategy()));
        createArchiveAndStartRecording();
    }

    private void createArchiveAndStartRecording() {
        final var controlRequestChannel = AERON_UDP_ENDPOINT + archiveHost + ":" + controlChannelPort;
        final var controlResponseChannel = AERON_UDP_ENDPOINT + "localhost" + ":3004";
        final var recordingEventsChannel = "aeron:udp?control-mode=dynamic|control=" + archiveHost + ":" + recordingEventsPort;

        log.info("ArchivePublisher connecting to archive. archiveHost {}. controlRequestChannel {}. " +
                        "controlResponseChannel {}. recordingEventsChannel {}. aeronDir {}",
                archiveHost, controlRequestChannel, controlResponseChannel, recordingEventsChannel, aeronDir);
        archive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .aeronDirectoryName(aeronDir)
                .controlRequestChannel(controlRequestChannel)
                .controlResponseChannel(controlResponseChannel)
                .recordingEventsChannel(recordingEventsChannel)
                .idleStrategy(new SleepingMillisIdleStrategy()));

        log.info("Creating publication on channel={}, streamId={}", CHANNEL, streamId);
        publication = aeron.addExclusivePublication(CHANNEL, streamId);

        startRecordings();
        archive.listRecordings(0, 1000, AeronUtils.logRecordingDescriptor(log));

        setupMonitoring();
        currentState = State.ARCHIVE_READY;
        log.info("ArchivePublisher is now ARCHIVE_READY");
    }

    private void startRecordings() {
        log.info("Starting recording on channel={}, streamId={}", CHANNEL, streamId);
        try {
            if (!AeronUtils.recordingExists(archive, CHANNEL, streamId)) {
                archive.startRecording(CHANNEL, streamId, SourceLocation.LOCAL);
                log.info("Started recording on {}, streamId={}", CHANNEL, streamId);
            } else {
                log.info("Recording already exists for {}, streamId={}", CHANNEL, streamId);
            }
        } catch (ArchiveException ex) {
            if (ex.getMessage() != null && ex.getMessage().contains("recording exists")) {
                log.info("Recording already exists for streamId={}, continuing...", streamId);
            } else {
                throw ex; // unexpected error, rethrow
            }
        }

        // Wait for the archive to see that session
        final var counters = aeron.countersReader();
        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        while (CountersReader.NULL_COUNTER_ID == counterId) {
            IDLE_STRATEGY.idle();
            counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        }
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        log.info("Recording session started: recordingId={}, sessionId={}", recordingId, publication.sessionId());
    }

    public void publish(DirectBuffer buffer, int offset, int length) {
        long result;
        int attempt = 1;
        do {
            result = publication.offer(buffer, offset, length);
            if (result < 0) {
                if (result == Publication.BACK_PRESSURED || result == Publication.NOT_CONNECTED && requireConnectedConsumer) {
                    int sleepMs = attempt * 100;
                    attempt++;
                    if (result == Publication.BACK_PRESSURED) {
                        log.error("[{}] Publication back pressure or not connected. Sleeping {}ms", name, sleepMs);
                    } else {
                        log.error("[{}] Publication not connected. Sleeping {}ms", name, sleepMs);
                    }
                    sleep(sleepMs);
                } else {
                    log.error("[{}] Publication error: {}", name, result);
                    break;
                }
            }
        } while (result < 0);
//        if (log.isDebugEnabled()) {
//            log.debug("[{}] Published msg len={}. Result: {}", name, length, result);
//        }
        log.info("[{}] Published msg len={}. Result: {}", name, length, result);
    }

    private void setupMonitoring() {
        if (log.isDebugEnabled()) {
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

            scheduler = Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, "ArchiveConsumerAgent-Cron"));

            scheduler.scheduleAtFixedRate(
                    () -> {
                        if (recordingEventsAdapter != null) {
                            recordingEventsAdapter.poll();
                        }
                        if (recordingSignalAdapter != null) {
                            recordingSignalAdapter.poll();
                        }
                    },
                    1000, // initial delay
                    1000, // repeat interval
                    TimeUnit.MILLISECONDS
            );
        }
    }

    public void close() {
        log.info("ArchivePublisher shutting down");
        currentState = State.SHUTTING_DOWN;
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
        closeScheduler();
    }

    private void errorHandler(Throwable throwable) {
        log.error("Unexpected error in ArchivePublisher: {}", throwable.getMessage(), throwable);
    }

    public String name() {
        return name;
    }

    public static class ArchiveActivityListener implements ControlEventListener, RecordingSignalConsumer {
        private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveActivityListener.class);

        @Override
        public void onResponse(final long controlSessionId, final long correlationId, final long relevantId,
                               final ControlResponseCode code, final String errorMessage) {
            LOGGER.info("code={} error={}", code, errorMessage);
        }

        @Override
        public void onSignal(final long controlSessionId, final long correlationId, final long recordingId,
                             final long subscriptionId, final long position, final RecordingSignal signal) {
            LOGGER.info("recordingId={} position={}, signal={}", recordingId, position, signal);
        }
    }

    public static class ArchiveProgressListener implements RecordingEventsListener {
        private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveProgressListener.class);

        @Override
        public void onStart(final long recordingId, final long startPosition, final int sessionId, final int streamId,
                            final String channel, final String sourceIdentity) {
            LOGGER.info("recording started recordingId={} startPos={}", recordingId, startPosition);
        }

        @Override
        public void onProgress(final long recordingId, final long startPosition, final long position) {
            LOGGER.info("recording activity recordingId={} startPos={} position={}", recordingId, startPosition,
                    position);
        }

        @Override
        public void onStop(final long recordingId, final long startPosition, final long stopPosition) {
            LOGGER.info("recording stopped recordingId={} startPos={} stopPos={}", recordingId, startPosition,
                    stopPosition);
        }
    }

    private void closeScheduler() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception exception) {
                log.error("Error shutting down scheduler", exception);
            }
        }
    }

    private enum State {
        INIT,
        ARCHIVE_READY,
        PUBLISHING,
        SHUTTING_DOWN
    }



}