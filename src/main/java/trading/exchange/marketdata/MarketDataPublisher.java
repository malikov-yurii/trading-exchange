package trading.exchange.marketdata;

import trading.api.MarketUpdate;
import trading.common.LFQueue;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MarketDataPublisher {
    private static final Logger log = LoggerFactory.getLogger(MarketDataPublisher.class);

    private final LFQueue<MarketUpdate> marketUpdateLFQueue;
    private final LFQueue<MarketUpdate> sequencedMarketUpdates;
    private final Aeron aeron;
    private final Publication publication;
    private final AtomicLong msgSeqNum = new AtomicLong();

    /**
     * In a production setup, you might pass in a pre-existing Aeron context or MediaDriver.
     * For a minimal example, we embed a MediaDriver and create a new Aeron instance.
     */
    public MarketDataPublisher(LFQueue<MarketUpdate> marketUpdateLFQueue,
                               LFQueue<MarketUpdate> sequencedMarketUpdates, String channel,
                               int streamId) {
        this.marketUpdateLFQueue = marketUpdateLFQueue;
        this.sequencedMarketUpdates = sequencedMarketUpdates;

        // Start an embedded MediaDriver if needed
        MediaDriver.Context mediaCtx = new MediaDriver.Context();
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaCtx);

        // Connect an Aeron client
        Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        this.aeron = Aeron.connect(aeronCtx);

        // Create a publication on the given channel + stream
        this.publication = aeron.addPublication(channel, streamId);

        // Subscribe to local LFQueue updates, then publish them out via Aeron
        this.marketUpdateLFQueue.subscribe(this::publish);
        log.info("MarketDataPublisher initialized");
    }

    /**
     * Called whenever a new MarketUpdate arrives on local queue.
     */
    private void publish(MarketUpdate marketUpdate) {
        if (marketUpdate == null) {
            log.warn("Null MarketUpdate received");
            return;
        }
        marketUpdate.setSeqNum(msgSeqNum.getAndIncrement());
        this.sequencedMarketUpdates.offer(marketUpdate);

        // Serialize MarketUpdate into a direct buffer
        // Minimal example: 1 byte type, 1 byte side, 6 longs.
        // Adjust as needed for your format.
        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
        int offset = 0;

        // seqNum
        buffer.putLong(offset, marketUpdate.getSeqNum());
        offset += Long.BYTES;

        // type ordinal
        buffer.putByte(offset, (byte) marketUpdate.getType().ordinal());
        offset += 1;

        // side ordinal
        buffer.putByte(offset, (byte) marketUpdate.getSide().ordinal());
        offset += 1;

        buffer.putLong(offset, marketUpdate.getOrderId());
        offset += Long.BYTES;
        buffer.putLong(offset, marketUpdate.getTickerId());
        offset += Long.BYTES;
        buffer.putLong(offset, marketUpdate.getPrice());
        offset += Long.BYTES;
        buffer.putLong(offset, marketUpdate.getQty());
        offset += Long.BYTES;
        buffer.putLong(offset, marketUpdate.getPriority());
        offset += Long.BYTES;

        // Attempt to publish
        long result;
        do {
            result = publication.offer(buffer, 0, offset);
            if (result < 0) {
                if (result == Publication.BACK_PRESSURED || result == Publication.NOT_CONNECTED) {
                    if (log.isDebugEnabled()) {
                        log.debug("Publication back pressure or not connected: {}", result);
                    }
                    // We can busy-wait or sleep briefly
                    Thread.yield();
                } else {
                    log.error("Publication error: {}", result);
                    break;
                }
            }
        } while (result < 0);

        log.info("Published {}", marketUpdate);
    }

    /**
     * Shutdown method, if needed.
     */
    public void close() {
        publication.close();
        aeron.close();
        // If you launched an embedded MediaDriver, keep a reference to it and close here
    }
}