package aeron;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import org.agrona.collections.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class AeronUtils {
    private static final Logger log = LoggerFactory.getLogger(AeronUtils.class);

    public static RecordingDescriptorConsumer logRecordingDescriptor(Logger log) {
        return (controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity) ->
                log.info("found recordingId={} startPos={} stopPos={} sessionId={} streamId={}",
                        recordingId, startPosition, stopPosition, sessionId, streamId);
    }

    public static RecordingDescriptorConsumer logRecordingDescriptor() {
        return logRecordingDescriptor(log);
    }

    public static RecordingDescriptorConsumer fullLogRecordingDescriptor(Logger log) {
        return fullLogRecordingDescriptor(log);
    }

    public static RecordingDescriptorConsumer fullLogRecordingDescriptor() {
        return (controlSessionId,
                correlationId,
                recordingId1,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity) -> log.info("Recording descriptor " +
                        "controlSessionId={}, " +
                        "correlationId={}, " +
                        "recordingId={}, " +
                        "startTimestamp={}, " +
                        "stopTimestamp={}, " +
                        "startPosition={}, " +
                        "stopPosition={}, " +
                        "initialTermId={}, " +
                        "segmentFileLength={}, " +
                        "termBufferLength={}, " +
                        "mtuLength={}, " +
                        "sessionId={}, " +
                        "streamId={}, " +
                        "strippedChannel={}, " +
                        "originalChannel={}, " +
                        "sourceIdentity={}",
                controlSessionId,
                correlationId,
                recordingId1,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity);
    }

    /**
     * Utility method to find local IPv4 address from eth0 (if possible), otherwise fallback
     */
    public static String localHost(final String fallback) {
        try {
            final Enumeration<NetworkInterface> interfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnumeration.hasMoreElements()) {
                final var networkInterface = interfaceEnumeration.nextElement();
                if (networkInterface.getName().startsWith("eth0")) {
                    final Enumeration<InetAddress> interfaceAddresses = networkInterface.getInetAddresses();
                    while (interfaceAddresses.hasMoreElements()) {
                        final InetAddress address = interfaceAddresses.nextElement();
                        if (address instanceof Inet4Address) {
                            String hostAddress = ((Inet4Address) address).getHostAddress();
                            log.info("Detected IPv4 address: {}", hostAddress);
                            return hostAddress;
                        }
                    }
                }
            }
        } catch (SocketException e) {
            log.warn("Unable to resolve localHost from eth0. Using fallback: {}", fallback);
        }
        return fallback;
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean recordingExists(AeronArchive archive, String channel, int streamId) {
        MutableBoolean exists = new MutableBoolean(false);

        archive.listRecordingsForUri(
                0, // fromRecordingId
                100, // recordCount to search through
                channel, // channelFragment (can be partial)
                streamId,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                 startPosition, stopPosition, initialTermId, segmentFileLength,
                 termBufferLength, mtuLength, sessionId, streamId1,
                 strippedChannel, originalChannel, sourceIdentity) -> {
                    if (originalChannel.contains(channel) && streamId1 == streamId) {
                        exists.set(true);
                    }
                }
        );

        return exists.get();
    }

    public static String getAeronDirRemote() {
        return System.getenv().getOrDefault("AERON_DIR", "/dev/shm/aeron-root/dir");
    }

    public static String getAeronDirLocal() {
        return "/dev/shm/aeron-local/dir";
    }
}
