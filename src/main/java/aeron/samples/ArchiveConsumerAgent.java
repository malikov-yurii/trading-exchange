package aeron.samples;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

public class ArchiveConsumerAgent implements Agent {
    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
    private static final int RECORDED_STREAM_ID = 100;
    private static final int REPLAY_STREAM_ID = 200;
    private static final Logger log = LoggerFactory.getLogger(ArchiveConsumerAgent.class);

    private final String archiveHost;
    private final MediaDriver mediaDriver;
    private final String thisHost;
    private final int archiveControlPort;
    private final int archiveEventPort;
    private final ArchiveConsumerFragmentHandler fragmentHandler;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy;
    private AeronArchive archive;
    private AeronArchive.AsyncConnect asyncConnect;
    private State currentState;

    // For replaying recordings sequentially:
    private List<RecordingInfo> recordings = null;
    private int currentRecordingIndex = 0;
    // For old recordings the stop position marks when the replay is finished.
    // For the latest recording replayed with Long.MAX_VALUE, we leave it as Long.MAX_VALUE.
    private long currentReplayStopPosition = -1;
    private Subscription replayDestinationSubs;

    public ArchiveConsumerAgent(final String archiveHost, final String thisHost, final int archiveControlPort,
                                final int archiveEventPort, final ArchiveConsumerFragmentHandler fragmentHandler) {
        this.archiveHost = archiveHost;
        this.thisHost = localHost(thisHost);
        this.archiveControlPort = archiveControlPort;
        this.archiveEventPort = archiveEventPort;
        this.fragmentHandler = fragmentHandler;
        this.idleStrategy = new SleepingMillisIdleStrategy(250);

        log.info("Launching media driver");
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

        log.info("Connecting Aeron; media driver directory {}", mediaDriver.aeronDirectoryName());
        this.aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .idleStrategy(new SleepingMillisIdleStrategy()));

        this.currentState = State.AERON_READY;
    }

    @Override
    public int doWork() {
        switch (currentState) {
            case AERON_READY -> connectToArchive();
            case POLLING_SUBSCRIPTION -> {
                // Poll fragments from the current replay subscription.
                replayDestinationSubs.poll(fragmentHandler, 100);

                // For old recordings (finite replay length), check if replay has finished.
                if (currentReplayStopPosition != Long.MAX_VALUE) {
                    boolean finished = false;
                    for (int i = 0, size = replayDestinationSubs.imageCount(); i < size; i++) {
                        final Image image = replayDestinationSubs.imageAtIndex(i);
                        if (image.position() >= currentReplayStopPosition) {
                            finished = true;
                            break;
                        }
                    }
                    if (finished) {
                        log.info("Finished replaying recording {}",
                                recordings.get(currentRecordingIndex).recordingId);
                        // Close current subscription before starting next replay.
                        CloseHelper.quietClose(replayDestinationSubs);
                        replayDestinationSubs = null;
                        currentRecordingIndex++;
                        if (currentRecordingIndex < recordings.size()) {
                            boolean isLatest = (currentRecordingIndex == recordings.size() - 1);
                            startReplayForRecording(recordings.get(currentRecordingIndex), isLatest);
                        } else {
                            // This branch should not be reached as we start the last recording in latest mode.
                            log.warn("No more recordings to replay");
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
            log.info("Connecting to Aeron archive");
            asyncConnect = AeronArchive.asyncConnect(new AeronArchive.Context()
                    .controlRequestChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveControlPort)
                    .recordingEventsChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveEventPort)
                    .controlResponseChannel(AERON_UDP_ENDPOINT + thisHost + ":0")
                    .aeron(aeron));
        } else {
            if (archive == null) {
                log.info("Awaiting Aeron archive connection");
                idleStrategy.idle();
                try {
                    archive = asyncConnect.poll();
                } catch (final TimeoutException e) {
                    log.info("Timeout connecting to archive, retrying");
                    asyncConnect = null;
                }
            } else {
                // Once archive is connected, list all recordings (if not done already) and start replay.
                if (recordings == null) {
                    recordings = getRecordings("aeron:ipc", RECORDED_STREAM_ID);
                    if (recordings.isEmpty()) {
                        log.info("No recordings found, idling...");
                        idleStrategy.idle();
                        return;
                    }
                    currentRecordingIndex = 0;
                    // If only one recording exists, treat it as the latest (tailing) replay.
                    boolean isLatest = (recordings.size() == 1);
                    startReplayForRecording(recordings.get(currentRecordingIndex), isLatest);
                    currentState = State.POLLING_SUBSCRIPTION;
                }
            }
        }
    }

    /**
     * Starts replay for the given recording.
     *
     * @param recording the recording info to replay.
     * @param isLatest  if true, use Long.MAX_VALUE as replay length to continue tailing new messages.
     */
    private void startReplayForRecording(final RecordingInfo recording, final boolean isLatest) {
        if (replayDestinationSubs != null) {
            CloseHelper.quietClose(replayDestinationSubs);
        }
        final var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + thisHost + ":0";
        replayDestinationSubs = aeron.addSubscription(localReplayChannelEphemeral, REPLAY_STREAM_ID);
        final var actualReplayChannel = replayDestinationSubs.tryResolveChannelEndpointPort();
        final long replayLength = isLatest ? Long.MAX_VALUE : (recording.stopPosition - recording.startPosition);
        currentReplayStopPosition = isLatest ? Long.MAX_VALUE : recording.stopPosition;
        final long replaySession = archive.startReplay(recording.recordingId,
                recording.startPosition, replayLength, actualReplayChannel, REPLAY_STREAM_ID);
        log.info("Replaying recordingId {} from {} to {} (isLatest: {}), replaySession {}",
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
    private List<RecordingInfo> getRecordings(final String remoteRecordedChannel, final int remoteRecordedStream) {
        final List<RecordingInfo> recordingList = new ArrayList<>();
        final RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId,
                                                      startTimestamp, stopTimestamp, startPosition,
                                                      stopPosition, initialTermId, segmentFileLength,
                                                      termBufferLength, mtuLength, sessionId,
                                                      streamId, strippedChannel, originalChannel,
                                                      sourceIdentity) -> {
            log.info("Found recordingId={} startPos={} stopPos={} sessionId={} streamId={}",
                    recordingId, startPosition, stopPosition, sessionId, streamId);
            recordingList.add(new RecordingInfo(recordingId, startPosition, stopPosition));
        };

        final int foundCount = archive.listRecordingsForUri(0, 100, remoteRecordedChannel,
                remoteRecordedStream, consumer);
        log.info("Total recordings found: {}", foundCount);

        // Sort recordings by their start position so that the oldest is replayed first.
        recordingList.sort(Comparator.comparingLong(r -> r.startPosition));
        return recordingList;
    }

    @Override
    public String roleName() {
        return "archive-client";
    }

    @Override
    public void onStart() {
        Agent.super.onStart();
        log.info("ArchiveClientAgent starting");
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
                            log.info("Detected IPv4 address: {}", inet4Address.getHostAddress());
                            return inet4Address.getHostAddress();
                        }
                    }
                }
            }
        } catch (final SocketException e) {
            log.info("Failed to get local host address", e);
        }
        return fallback;
    }

    @Override
    public void onClose() {
        Agent.super.onClose();
        log.info("Shutting down ArchiveClientAgent");
        CloseHelper.quietClose(replayDestinationSubs);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }

    private enum State {
        AERON_READY,
        POLLING_SUBSCRIPTION
    }

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