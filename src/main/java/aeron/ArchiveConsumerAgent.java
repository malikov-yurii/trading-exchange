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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    private final AtomicReference<List<RecordingInfo>> recordings = new AtomicReference<>();

    private int currentRecordingIndex = 0;
    // For old recordings the stop position marks when the replay is finished.
    // For the latest recording replayed with Long.MAX_VALUE, we leave it as Long.MAX_VALUE.
    private volatile long currentReplayStopPosition = -1;
    private volatile long lastImagePosition = -1;
    private Subscription replayDestinationSubscription;

    private final ReplayStrategy replayStrategy;

    private ScheduledExecutorService scheduler;
    private final List<Runnable> onReplayFinish = new ArrayList<>();
    private boolean isOldMsgReplayInProgress = true;

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
        this.replayStrategy = replayStrategy;

        log.info("{} | Starting {}. streamId {}, THISHOST {}, ARCHIVEHOST {}, CONTROLPORT {}, EVENTSPORT {}",
                this.name, this.replayStrategy, this.streamId, archiveHost, controlPort, thisHost, eventPort);
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
    }

    public void onReplayOldFinish(Runnable onReplayFinishCallback) {
        this.onReplayFinish.add(onReplayFinishCallback);
    }

    public static void errorHandler(final Throwable throwable) {
        log.error("agent error {}", throwable.getMessage(), throwable);
    }

    @Override
    public int doWork() {
        switch (currentState) {
            case AERON_READY:
                connectToArchive();
                break;

            case POLLING_SUBSCRIPTION:
                if (replayDestinationSubscription == null) {
                    startRecordingReplay(currentRecordingIndex);
                    return 0;
                } else {
                    // Poll fragments from the current replay subscription.
                    replayDestinationSubscription.poll(fragmentHandler, 100);

                    // For old recordings (finite replay length), check if replay has finished.
                    if (currentReplayStopPosition != Long.MAX_VALUE) {
                        boolean finished = false;
                        for (int i = 0, size = replayDestinationSubscription.imageCount(); i < size; i++) {
                            final Image image = replayDestinationSubscription.imageAtIndex(i);
                            long newImagePosition = image.position();
                            if (newImagePosition != lastImagePosition) {
                                log.info("{} | newImagePosition {}. lastImagePosition {}. diff {}. currentReplayStopPosition {}. {}", this.name, newImagePosition,
                                        lastImagePosition, newImagePosition - lastImagePosition, currentReplayStopPosition, image);
                            }
                            lastImagePosition = newImagePosition;
                            if (lastImagePosition >= currentReplayStopPosition) {
                                finished = true;
                                break;
                            }
                        }
                        if (finished) {
                            RecordingInfo finishedRecording = recordings().get(currentRecordingIndex);
                            log.info("{} | Finished replaying recording {}", this.name, finishedRecording.recordingId);
                            startRecordingReplay(++currentRecordingIndex);
                        }
                    }
                }
                break;

            default:
                log.error("Unknown state {}", currentState);
                break;
        }
        return 0;
    }

    private void startRecordingReplay(int recordingInd) {
        stopCurrentRecordingReplay();
        if (recordingInd >= recordings().size()) {
            if (isOldMsgReplayInProgress) {
                onReplayFinish();
            } else {
                idleStrategy.idle();
            }
            return;
        }

        final var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + thisHost + ":0";
        replayDestinationSubscription = aeron.addSubscription(localReplayChannelEphemeral, REPLAY_STREAM_ID);
        final var actualReplayChannel = replayDestinationSubscription.tryResolveChannelEndpointPort();

        final RecordingInfo recording = recordings().get(recordingInd);
        boolean isActiveRecording = recording.stopPosition == -1;
        final long replayLength = isActiveRecording ? Long.MAX_VALUE : (recording.stopPosition - recording.startPosition);
        setCurrentReplayStopPosition(isActiveRecording ? Long.MAX_VALUE : recording.stopPosition);

        if (replayLength <= 0) {
            log.info("{} | Skipping Replay for recordingId {}. replayLength {}, startPos {} stopPos {}",
                    this.name, recording.recordingId, replayLength, recording.startPosition, recording.stopPosition);
            if (recordingInd < recordings().size() - 1) {
                startRecordingReplay(++recordingInd);
            } else {
                onReplayFinish();
            }
            return;
        }

        final long replaySession = archive.startReplay(recording.recordingId,
                recording.startPosition, replayLength, actualReplayChannel, REPLAY_STREAM_ID);

        log.info("{} | Replaying recordingId {} from {} to {} (isActiveRecording: {} isLatest: {}), replaySession {}", this.name,
                recording.recordingId, recording.startPosition, recording.stopPosition, isActiveRecording, isCurrentRecordingLatest(), replaySession);
    }

    private boolean onReplayFinish() {
        if (replayStrategy == ReplayStrategy.REPLAY_OLD) {
            log.info("{} | Finished replaying old recordings. Stopping consumer.", this.name);
            onClose();
            return true;
        }
        this.isOldMsgReplayInProgress = false;
        this.onReplayFinish.forEach(Runnable::run);
        log.info("{} | Finished replaying old recordings, but waiting for new ones", this.name);
        return false;
    }

    private void stopCurrentRecordingReplay() {
        if (replayDestinationSubscription != null) {
            CloseHelper.quietClose(replayDestinationSubscription);
            replayDestinationSubscription = null;
            log.info("{} | stopCurrentRecordingReplay. Resetting currentReplayStopPosition {}, lastImagePosition {}",
                    this.name, currentReplayStopPosition, lastImagePosition);
            currentReplayStopPosition = -1;
            lastImagePosition = -1;
        }
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
                if (recordings() == null) {
                    updateRecordings();

                    scheduler = Executors.newSingleThreadScheduledExecutor(
                            r -> new Thread(r, "ArchiveConsumerAgent-Cron"));

                    scheduler.scheduleAtFixedRate(
                            this::updateRecordings,
                            1000, // initial delay
                            1000, // repeat interval
                            TimeUnit.MILLISECONDS
                    );

                    if (recordings().isEmpty()) {
                        log.info("{} | No recordings found", this.name);
                        if (onReplayFinish()) {
                            return;
                        }
                    }
                    currentState = State.POLLING_SUBSCRIPTION;
                }
            }
        }
    }

    private void updateRecordings() {
        List<RecordingInfo> fetchedRecordings = fetchRecordings();
        if (!fetchedRecordings.equals(recordings())) {
            log.info("{} | Fetched recordings: {}, recordings(): {}, currentRecordingIndex: {}",
                    this.name, fetchedRecordings, recordings(), currentRecordingIndex);
        }

        if (recordings() != null
                && currentRecordingIndex >= 0
                && currentRecordingIndex < recordings().size()
                && recordings().get(currentRecordingIndex).stopPosition < 0) {

            RecordingInfo prevCurrRecordingMeta = recordings().get(currentRecordingIndex);

            RecordingInfo newLatestRecording = fetchedRecordings.stream()
                    .filter(r -> r.recordingId == prevCurrRecordingMeta.recordingId).findFirst().orElse(null);

            if (newLatestRecording != null && newLatestRecording.stopPosition > 0) {
                log.info("{} | Latest newLatestRecording recordingId {} finished", this.name, newLatestRecording.recordingId);
                long stopPosition = newLatestRecording.stopPosition;
                setCurrentReplayStopPosition(stopPosition);
                if (lastImagePosition != -1 && lastImagePosition >= currentReplayStopPosition) {
                    log.info("{} | Latest newLatestRecording already fully replayed. " +
                                    "lastImagePosition {}. currentReplayStopPosition {}. {}", this.name,
                            lastImagePosition, currentReplayStopPosition, recordings().get(recordings().size() - 1).recordingId);
                    onReplayFinish();
                }
            } else if (prevCurrRecordingMeta.stopPosition() < 0  && replayStrategy == ReplayStrategy.REPLAY_OLD) {
                log.info("{} | Latest newLatestRecording not finished yet", this.name);
            }
        }

        if (recordings() == null || !recordings().equals(fetchedRecordings)) {
            log.info("Updated recordings to {}", fetchedRecordings);
            this.recordings.set(fetchedRecordings);
        }
    }

    private List<RecordingInfo> fetchRecordings() {
        final List<RecordingInfo> recordingList = new ArrayList<>();
        final RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId,
                                                      startTimestamp, stopTimestamp, startPosition,
                                                      stopPosition, initialTermId, segmentFileLength,
                                                      termBufferLength, mtuLength, sessionId,
                                                      streamId1, strippedChannel, originalChannel,
                                                      sourceIdentity) -> {
            RecordingInfo updatedRecordingMeta = new RecordingInfo(recordingId, startPosition, stopPosition);

            recordingList.add(updatedRecordingMeta);

            if (log.isDebugEnabled()) {
                log.debug("{} | Found startTimestamp={} recordingId={} startPos={} stopPos={} sessionId={} streamId={}", this.name,
                        Instant.ofEpochMilli(startTimestamp), recordingId, startPosition, stopPosition, sessionId, streamId1);
            } else if (recordings() == null || recordingList.size() > recordings().size() ||
                    recordings().get(recordingList.size() - 1).stopPosition != recordingList.get(recordingList.size() - 1).stopPosition) {
                log.info("{} | Found startTimestamp={} recordingId={} startPos={} stopPos={} sessionId={} streamId={}", this.name,
                        Instant.ofEpochMilli(startTimestamp), recordingId, startPosition, stopPosition, sessionId, streamId1);
            }
        };

        final int foundCount = archive.listRecordingsForUri(0, 100, "aeron:ipc",
                streamId, consumer);
        log.debug("{} | Total recordings found: {}", this.name, foundCount);

        // Sort recordings by their start position so that the oldest is replayed first.
        // Or sort by r.startTimestamp
        recordingList.sort(Comparator.comparingLong(r -> r.recordingId));
        return recordingList;
    }

    private List<RecordingInfo> recordings() {
        return this.recordings.get();
    }

    private void setCurrentReplayStopPosition(long stopPosition) {
        log.info("{} | Setting currentReplayStopPosition to {}", this.name, stopPosition);
        currentReplayStopPosition = stopPosition;
    }

    private boolean isCurrentRecordingLatest() {
        return currentRecordingIndex == recordings().size() - 1;
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
                        if (addr instanceof Inet4Address) {
                            Inet4Address inet4Address = (Inet4Address) addr;
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

    private record RecordingInfo (long recordingId, long startPosition, long stopPosition) {}
}