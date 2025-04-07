package aeron;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.FragmentHandler;
import lombok.Data;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ArchiveConsumerAgent implements Agent {
    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
    private static final int REPLAY_STREAM_ID = 200;
    private static final Logger log = LoggerFactory.getLogger(ArchiveConsumerAgent.class);

    private final String archiveHost;
    private final String thisHost;
    private final int archiveControlPort;
    private final int archiveEventPort;
    private final String name;
    private final int streamId;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private AeronArchive archive;

    private final FragmentHandler fragmentHandler;
    private final IdleStrategy idleStrategy;

    private AeronArchive.AsyncConnect asyncConnect;
    private State currentState;

    // For replaying recordings sequentially:
    private List<RecordingInfo> recordings = null;

    private int currentRecordingIndex = 0;
    // For old recordings the stop position marks when the replay is finished.
    // For the latest recording replayed with Long.MAX_VALUE, we leave it as Long.MAX_VALUE.
    private volatile long currentReplayStopPosition = -1;
    private volatile long lastImagePosition;
    private Subscription replayDestinationSubscription;
    private final ReplayStrategy replayStrategy;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;

    public ArchiveConsumerAgent(int streamId, final FragmentHandler fragmentHandler,
                                final ReplayStrategy replayStrategy, String name) {
        this.streamId = streamId;

        final var thisHost = System.getenv().get("THISHOST");
        final var archiveHost = System.getenv().get("ARCHIVEHOST");
        final var controlPort = System.getenv().get("CONTROLPORT");
        final var eventPort = System.getenv().get("EVENTSPORT");

        if (ObjectUtils.anyNull(archiveHost, controlPort, thisHost, eventPort)) {
            log.error("env vars required: THISHOST {}, ARCHIVEHOST {}, CONTROLPORT {}, EVENTSPORT {}",
                    archiveHost, controlPort, thisHost, eventPort);
            throw new IllegalArgumentException("Missing required environment variables");
        }
        this.name = name;

        this.archiveHost = archiveHost;
        this.thisHost = localHost(thisHost);
        this.archiveControlPort = Integer.parseInt(controlPort);
        this.archiveEventPort = Integer.parseInt(eventPort);
        this.fragmentHandler = fragmentHandler;
        this.idleStrategy = new SleepingMillisIdleStrategy(250);

        log.info("{} | Launching media driver", this.name);
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(AeronUtils.getAeronDirLocal())
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

        log.info("{} | Connecting Aeron; media driver directory {}", this.name, mediaDriver.aeronDirectoryName());
        this.aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .idleStrategy(new SleepingMillisIdleStrategy()));

        this.currentState = State.AERON_READY;
        this.replayStrategy = replayStrategy;
    }

    public static void errorHandler(final Throwable throwable) {
        log.error("agent error {}", throwable.getMessage(), throwable);
    }

    @Override
    public int doWork() {
        switch (currentState) {
            case AERON_READY -> connectToArchive();
            case POLLING_SUBSCRIPTION -> {
                // Poll fragments from the current replay subscription.
                replayDestinationSubscription.poll(fragmentHandler, 100);

                // For old recordings (finite replay length), check if replay has finished.
                if (currentReplayStopPosition != Long.MAX_VALUE) {
                    boolean finished = false;
                    for (int i = 0, size = replayDestinationSubscription.imageCount(); i < size; i++) {
                        final Image image = replayDestinationSubscription.imageAtIndex(i);
                        lastImagePosition = image.position();
                        if (lastImagePosition >= currentReplayStopPosition) {
                            finished = true;
                            break;
                        }
                    }
                    if (finished) {
                        log.info("{} | Finished replaying recording {}", this.name,
                                recordings.get(currentRecordingIndex).recordingId);
                        // Close current subscription before starting next replay.
                        CloseHelper.quietClose(replayDestinationSubscription);
                        replayDestinationSubscription = null;
                        currentRecordingIndex++;
                        if (currentRecordingIndex < recordings.size()) {
                            boolean isLatest = isCurrentRecordingLatest();
                            startReplayForRecording(recordings.get(currentRecordingIndex), isLatest);
                        } else {
                            // This branch should not be reached as we start the last recording in latest mode.
                            log.warn("No more recordings to replay");
                            onClose();
                        }
                    }
                }
            }
            default -> log.error("Unknown state {}", currentState);
        }
        return 0;
    }

    private void connectToArchive() {
        if (asyncConnect == null) {
            log.info("{} | Connecting to Aeron archive", this.name);
            asyncConnect = AeronArchive.asyncConnect(new AeronArchive.Context()
                    .controlRequestChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveControlPort)
                    .recordingEventsChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveEventPort)
                    .controlResponseChannel(AERON_UDP_ENDPOINT + thisHost + ":0")
                    .aeron(aeron));
        } else {
            if (archive == null) {
                log.info("{} | Awaiting Aeron archive connection", this.name);
                idleStrategy.idle();
                try {
                    archive = asyncConnect.poll();
                } catch (final TimeoutException e) {
                    log.info("{} | Timeout connecting to archive, retrying", this.name);
                    asyncConnect = null;
                }
            } else {
                // Once archive is connected, list all recordings (if not done already) and start replay.
                if (recordings == null) {
                    recordings = fetchRecordings();
                    if (recordings.isEmpty()) {
                        log.info("{} | No recordings found, stopping...", this.name);
                        onClose();
                        return;
                    }
                    currentRecordingIndex = 0;
                    // If only one recording exists, treat it as the latest (tailing) replay.
                    boolean isLatest = isCurrentRecordingLatest();
                    startReplayForRecording(recordings.get(currentRecordingIndex), isLatest);
                    currentState = State.POLLING_SUBSCRIPTION;

                    var lastRec = recordings.getLast();
                    if (replayStrategy == ReplayStrategy.REPLAY_OLD && lastRec.stopPosition < 0) {
                        log.info("{} | Replay strategy is REPLAY_OLD, but last recording stop position is {}. ", this.name,
                                lastRec.stopPosition);
                        // means the latest recording has not yet finished, and we need to wait for it to finish

                        scheduler = Executors.newSingleThreadScheduledExecutor(
                                r -> new Thread(r, "ArchiveConsumerAgent-Cron"));

                        scheduledFuture = scheduler.scheduleAtFixedRate(
                                this::checkLatestRecordingFinished,
                                1000, // initial delay
                                1000, // repeat interval
                                TimeUnit.MILLISECONDS
                        );
                    }

                }
            }
        }
    }

    private List<RecordingInfo> fetchRecordings() {
        return fetchRecordings("aeron:ipc", streamId);
    }

    private void checkLatestRecordingFinished() {
        List<RecordingInfo> recordingInfos = fetchRecordings();
        RecordingInfo last = this.recordings.getLast();
        recordingInfos.stream().filter(r -> r.recordingId == last.recordingId).findFirst().ifPresent(recording -> {
            if (recording.stopPosition > 0) {
                log.info("{} | Latest recording recordingId {} finished, stopping scheduler", this.name, recording.recordingId);
                scheduledFuture.cancel(false);
                currentReplayStopPosition = recording.stopPosition;
                if (lastImagePosition >= currentReplayStopPosition) {
                    log.info("{} | Latest recording already fully replayed, stopping consumer. " +
                                    "lastImagePosition {}. currentReplayStopPosition {}. {}", this.name,
                            lastImagePosition, currentReplayStopPosition, this.recordings.getLast().recordingId);
                    onClose();
                }
            } else {
                log.info("{} | Latest recording not finished yet", this.name);
            }
        });
    }

    private boolean isCurrentRecordingLatest() {
        return currentRecordingIndex == recordings.size() - 1;
    }

    /**
     * Starts replay for the given recording.
     *
     * @param recording the recording info to replay.
     * @param isLatest  if true, use Long.MAX_VALUE as replay length to continue tailing new messages.
     */
    private void startReplayForRecording(final RecordingInfo recording, final boolean isLatest) {
        if (replayDestinationSubscription != null) {
            CloseHelper.quietClose(replayDestinationSubscription);
        }
        final var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + thisHost + ":0";
        replayDestinationSubscription = aeron.addSubscription(localReplayChannelEphemeral, REPLAY_STREAM_ID);
        final var actualReplayChannel = replayDestinationSubscription.tryResolveChannelEndpointPort();
        final long replayLength = isLatest ? Long.MAX_VALUE : (recording.stopPosition - recording.startPosition);
        currentReplayStopPosition = isLatest ? Long.MAX_VALUE : recording.stopPosition;
        final long replaySession = archive.startReplay(recording.recordingId,
                recording.startPosition, replayLength, actualReplayChannel, REPLAY_STREAM_ID);
        log.info("{} | Replaying recordingId {} from {} to {} (isLatest: {}), replaySession {}", this.name,
                recording.recordingId, recording.startPosition, recording.stopPosition, isLatest, replaySession);
    }

    /**
     * Queries the archive for all recordings for a given channel and stream, and sorts them in ascending order
     * by their start position.
     *
     * @param remoteRecordedChannel the channel to query for.
     * @param remoteRecordedStream  the stream id.
     * @return a list of recordings found.
     */
    private List<RecordingInfo> fetchRecordings(final String remoteRecordedChannel, final int remoteRecordedStream) {
        final List<RecordingInfo> recordingList = new ArrayList<>();
        final RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId,
                                                      startTimestamp, stopTimestamp, startPosition,
                                                      stopPosition, initialTermId, segmentFileLength,
                                                      termBufferLength, mtuLength, sessionId,
                                                      streamId, strippedChannel, originalChannel,
                                                      sourceIdentity) -> {
            log.info("{} | Found startTimestamp={} recordingId={} startPos={} stopPos={} sessionId={} streamId={}", this.name,
                    Instant.ofEpochMilli(startTimestamp), recordingId, startPosition, stopPosition, sessionId, streamId);
            recordingList.add(new RecordingInfo(recordingId, startPosition, stopPosition));
        };

        final int foundCount = archive.listRecordingsForUri(0, 100, remoteRecordedChannel,
                remoteRecordedStream, consumer);
        log.info("{} | Total recordings found: {}", this.name, foundCount);

        // Sort recordings by their start position so that the oldest is replayed first.
        // Or sort by r.startTimestamp
        recordingList.sort(Comparator.comparingLong(r -> r.recordingId));
        return recordingList;
    }

    @Override
    public String roleName() {
        return "archive-client";
    }

    @Override
    public void onStart() {
        Agent.super.onStart();
        log.info("{} | ArchiveClientAgent starting", this.name);
    }

    public String localHost(final String fallback) {
        try {
            final Enumeration<NetworkInterface> interfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnumeration.hasMoreElements()) {
                final var networkInterface = interfaceEnumeration.nextElement();
                if (networkInterface.getName().startsWith("eth0")) {
                    final Enumeration<InetAddress> interfaceAddresses = networkInterface.getInetAddresses();
                    while (interfaceAddresses.hasMoreElements()) {
                        final InetAddress addr = interfaceAddresses.nextElement();
                        if (addr instanceof Inet4Address inet4Address) {
                            log.info("{} | Detected IPv4 address: {}", this.name, inet4Address.getHostAddress());
                            return inet4Address.getHostAddress();
                        }
                    }
                }
            }
        } catch (final SocketException e) {
            log.info(name + " Failed to get local host address", e);
        }
        return fallback;
    }

    @Override
    public void onClose() {
        Agent.super.onClose();
        log.info("{} | Shutting down ArchiveClientAgent", this.name);
        CloseHelper.quietClose(replayDestinationSubscription);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
        closeScheduler();
        Thread.currentThread().interrupt();
    }

    private void closeScheduler() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception exception) {
                log.error(name + " Error shutting down scheduler", exception);
            }
        }
    }

    private enum State {
        AERON_READY,
        POLLING_SUBSCRIPTION
    }

    public enum ReplayStrategy {
        REPLAY_OLD,
        REPLAY_OLD_AND_SUBSCRIBE
    }

    @Data
    private static class RecordingInfo {
        final long recordingId;
        final long startPosition;
        final long stopPosition;

        RecordingInfo(final long recordingId, final long startPosition, final long stopPosition) {
            this.recordingId = recordingId;
            this.startPosition = startPosition;
            this.stopPosition = stopPosition;
        }
    }
}