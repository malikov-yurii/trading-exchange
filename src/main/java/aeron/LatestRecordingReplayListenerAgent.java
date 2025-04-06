package aeron;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class LatestRecordingReplayListenerAgent implements Agent {
    public static final String AERON_UDP_ENDPOINT = "aeron:udp?endpoint=";
    private static final int RECORDED_STREAM_ID = 100;
    private static final int REPLAY_STREAM_ID = 200;
    private static final Logger log = LoggerFactory.getLogger(LatestRecordingReplayListenerAgent.class);
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
    private Subscription replayDestinationSubs;

    public LatestRecordingReplayListenerAgent(final String archiveHost, final String thisHost, final int archiveControlPort,
                                              final int archiveEventPort, final ArchiveConsumerFragmentHandler fragmentHandler) {
        this.archiveHost = archiveHost;
        this.thisHost = localHost(thisHost);
        this.archiveControlPort = archiveControlPort;
        this.archiveEventPort = archiveEventPort;
        this.fragmentHandler = fragmentHandler;
        this.idleStrategy = new SleepingMillisIdleStrategy(250);

        log.info("launching media driver");
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

        log.info("connecting aeron; media driver directory {}", mediaDriver.aeronDirectoryName());
        this.aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .idleStrategy(new SleepingMillisIdleStrategy()));

        this.currentState = State.AERON_READY;
    }

    @Override
    public int doWork() {
        switch (currentState) {
            case AERON_READY -> connectToArchive();
            case POLLING_SUBSCRIPTION -> replayDestinationSubs.poll(fragmentHandler, 100);
            default -> log.error("unknown state {}", currentState);
        }

        return 0;
    }

    private void connectToArchive() {
        //start an asyncConnect if one not in progress
        if (asyncConnect == null) {
            log.info("connecting aeron archive");
            asyncConnect = AeronArchive.asyncConnect(new AeronArchive.Context()
                    .controlRequestChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveControlPort)
                    .recordingEventsChannel(AERON_UDP_ENDPOINT + archiveHost + ":" + archiveEventPort)
                    .controlResponseChannel(AERON_UDP_ENDPOINT + thisHost + ":0")
                    .aeron(aeron));
        } else {
            if (null == archive) {
                log.info("awaiting aeron archive");
                idleStrategy.idle();
                try {
                    archive = asyncConnect.poll();
                } catch (final TimeoutException e) {
                    log.info("timeout");
                    asyncConnect = null;
                }
            } else {
                log.info("finding remote recording");
                //archive is connected. find the recording on the remote archive host
                final var recordingId = getRecordingId("aeron:ipc", RECORDED_STREAM_ID);
                if (recordingId != Long.MIN_VALUE) {
                    //ask aeron to assign an ephemeral port for this replay
                    final var localReplayChannelEphemeral = AERON_UDP_ENDPOINT + thisHost + ":0";
                    //construct a local subscription for the remote host to replay to
                    replayDestinationSubs = aeron.addSubscription(localReplayChannelEphemeral, REPLAY_STREAM_ID);
                    //resolve the actual port and use that for the replay
                    final var actualReplayChannel = replayDestinationSubs.tryResolveChannelEndpointPort();
                    log.info("actualReplayChannel={}", actualReplayChannel);
                    //replay from the archive recording the start
                    final long replaySession =
                            archive.startReplay(recordingId, 0L, Long.MAX_VALUE, actualReplayChannel, REPLAY_STREAM_ID);
                    log.info("ready to poll subscription, replaying recordingId {} to {}, image is {}",
                            recordingId, actualReplayChannel, (int) replaySession);
                    currentState = State.POLLING_SUBSCRIPTION;
                } else {
                    //await the remote host being ready, idle 250ms
                    idleStrategy.idle();
                }
            }
        }
    }

    private long getRecordingId(final String remoteRecordedChannel, final int remoteRecordedStream) {
        final var lastRecordingId = new MutableLong();
        final RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId,
                                                      startTimestamp, stopTimestamp, startPosition,
                                                      stopPosition, initialTermId, segmentFileLength,
                                                      termBufferLength, mtuLength, sessionId,
                                                      streamId, strippedChannel, originalChannel,
                                                      sourceIdentity) -> {
            log.info("found recordingId={} startPos={} stopPos={} sessionId={} streamId={}",
                    recordingId, startPosition, stopPosition, sessionId, streamId);
            lastRecordingId.set(recordingId);
        };

        final var fromRecordingId = 0L;
        final var recordCount = 100;

        final int foundCount = archive.listRecordingsForUri(fromRecordingId, recordCount, remoteRecordedChannel,
                remoteRecordedStream, consumer);
        log.info("Found total {} recordings", foundCount);

        if (0 == foundCount) {
            return Long.MIN_VALUE;
        }

        long lastRecording = lastRecordingId.get();
        log.info("lastRecordingId={}", lastRecording);
        return lastRecording;
    }

    @Override
    public String roleName() {
        return "archive-client";
    }

    @Override
    public void onStart() {
        Agent.super.onStart();
        log.info("starting");
    }

    public String localHost(final String fallback) {
        try {
            final Enumeration<NetworkInterface> interfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnumeration.hasMoreElements()) {
                final var networkInterface = interfaceEnumeration.nextElement();

                if (networkInterface.getName().startsWith("eth0")) {
                    final Enumeration<InetAddress> interfaceAddresses = networkInterface.getInetAddresses();
                    while (interfaceAddresses.hasMoreElements()) {
                        if (interfaceAddresses.nextElement() instanceof Inet4Address inet4Address) {
                            log.info("detected ip4 address as {}", inet4Address.getHostAddress());
                            return inet4Address.getHostAddress();
                        }
                    }
                }
            }
        } catch (final SocketException e) {
            log.info("Failed to get address");
        }
        return fallback;
    }

    @Override
    public void onClose() {
        Agent.super.onClose();
        log.info("shutting down");
        CloseHelper.quietClose(replayDestinationSubs);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }

}
