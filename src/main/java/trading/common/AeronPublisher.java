package trading.common;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static trading.common.Utils.env;

public class AeronPublisher {
    private static final Logger log = LoggerFactory.getLogger(AeronPublisher.class);
    public static final boolean PERSISTENT_STREAM = true;
    public static final boolean REQUIRE_CONNECTED_CONSUMER = false;

    private final Publication publication;
    private final String name;
    private final boolean requireConnectedConsumer;

    public AeronPublisher(String channel, int streamId, String name) {
        this(channel, streamId, name, false, false);
    }

    public AeronPublisher(String channel, int streamId, String name, boolean persistentStream, boolean requireConnectedConsumer) {
        this.requireConnectedConsumer = requireConnectedConsumer;
        this.name = name;

        if (persistentStream) {
//            String controlRequestChannel = "aeron:udp?endpoint=127.0.0.1:8010";
            String controlRequestChannel = "aeron:udp?term-length=64k|endpoint=localhost:8010";
//            String controlResponseChannel = "aeron:udp?endpoint=127.0.0.1:0";
            String controlResponseChannel = "aeron:udp?term-length=64k|endpoint=localhost:0";
//            String controlResponseChannel = "aeron:ipc";
            int controlRequestStreamId = 100;
            int controlResponseStreamId = 101;
            log.info("--------------------> [{}] Creating AeronPublisher: channel: {}, streamId: {}, persistentStream: {}, requireConnectedConsumer: {}, aeronDir: {}" +
                            ", controlRequestChannel: {}, controlResponseChannel: {}, controlRequestStreamId: {}, controlResponseStreamId: {}",
                    name, channel, streamId, persistentStream, requireConnectedConsumer, env("AERON_DIR", null),
                    controlRequestChannel, controlResponseChannel, controlRequestStreamId, controlResponseStreamId);
            Aeron aeron = AeronClient.INSTANCE;

            final AeronArchive.Context ctx = new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel(controlRequestChannel)
                    .controlRequestStreamId(controlRequestStreamId)
                    .controlResponseChannel(controlResponseChannel)
                    .controlResponseStreamId(controlResponseStreamId);

            log. info("aeron.context().aeronDirectoryName(): {}", aeron.context().aeronDirectoryName());
            AeronArchive archive = AeronArchive.connect(ctx);
            if (archive == null) {
                throw new IllegalStateException("Timed out waiting for Aeron Archive");
            }
            archive.startRecording(channel, streamId, SourceLocation.LOCAL);
            this.publication = aeron.addExclusivePublication(channel, streamId); // Exclusive publication (required for archive)
        } else {
            log.info("--------------------> [{}] Creating AeronPublisher: channel: {}, streamId: {}, persistentStream: {}, requireConnectedConsumer: {}, aeronDir: {}",
                    name, channel, streamId, persistentStream, requireConnectedConsumer, env("AERON_DIR", null));
            this.publication = AeronClient.INSTANCE.addPublication(channel, streamId);
        }
    }


//            AeronArchive.AsyncConnect async = AeronArchive.asyncConnect(ctx);
//            long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
//            AeronArchive archive;
//            while ((archive = async.poll()) == null && System.nanoTime() < deadlineNs) {
//                try {
//                    int millis = 100;
//                    log.info("AsyncConnect retry in {}ms", millis);
//                    Thread.sleep(millis);
//                } catch (InterruptedException e) {
//                }
//            }

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

    public void close() {
        publication.close();
        // If you launched an embedded MediaDriver, keep a reference to it and close here
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
