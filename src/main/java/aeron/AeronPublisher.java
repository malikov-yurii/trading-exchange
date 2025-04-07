package aeron;

import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AeronPublisher {
    private static final Logger log = LoggerFactory.getLogger(AeronPublisher.class);

    private final Publication publication;
    private final String name;
    private final boolean requireConnectedConsumer;

    public AeronPublisher(String channel, int streamId, String name) {
        this(channel, streamId, name, false);
    }

    public AeronPublisher(String channel, int streamId, String name, boolean requireConnectedConsumer) {
        this.requireConnectedConsumer = requireConnectedConsumer;
        this.name = name;

        log.info("--------------------> [{}] Creating AeronPublisher: channel: {}, streamId: {}, requireConnectedConsumer: {}, aeronDir: {}",
                name, channel, streamId, requireConnectedConsumer, AeronUtils.getAeronDirRemote());
        this.publication = AeronClient.AERON_INSTANCE_REMOTE.addPublication(channel, streamId);
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
                    AeronUtils.sleep(sleepMs);
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

}
