package trading.common;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AeronPublisher {
    private static final Logger log = LoggerFactory.getLogger(AeronPublisher.class);

    private final Aeron aeron;
    private final Publication publication;
    private final String name;

    public AeronPublisher(String channel, int streamId, String name) {
        log.info("--------------------> [{}] Creating AeronPublisher: channel: {}, streamId: {}", name, channel, streamId);
        this.name = name;

        MediaDriver.Context mediaCtx = new MediaDriver.Context();
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaCtx);

        Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());

        this.aeron = Aeron.connect(aeronCtx);
        this.publication = aeron.addPublication(channel, streamId);
    }

    public void publish(DirectBuffer buffer, int offset, int length) {
        long result;
        int attempt = 1;
        do {
            result = publication.offer(buffer, offset, length);
            if (result < 0) {
                if (result == Publication.BACK_PRESSURED || result == Publication.NOT_CONNECTED) {
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
        aeron.close();
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
