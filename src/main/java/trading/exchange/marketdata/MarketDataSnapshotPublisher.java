package trading.exchange.marketdata;

import trading.api.MarketUpdate;
import trading.api.MarketUpdateType;
import trading.common.LFQueue;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;

/**
 * MarketDataSnapshotPublisher:
 * - Receives incremental updates from marketUpdateLFQueue (like MarketDataPublisher).
 * - Maintains an internal snapshot of orders by ticker.
 * - Every 60 secs, publishes a full snapshot to a separate Aeron channel (snapshotChannel, snapshotStreamId).
 */
public class MarketDataSnapshotPublisher {

    private static final Logger log = LoggerFactory.getLogger(MarketDataSnapshotPublisher.class);

    // For each tickerId, store a map of orderId -> MarketUpdate
    private final List<Map<Long, MarketUpdate>> tickerOrders;

    // The last seq number we processed from incremental feed
    private long lastIncSeqNum = -1L;

    // The queue carrying incremental updates for incremental feed
    private final LFQueue<MarketUpdate> marketUpdateLFQueue;

    // The Aeron publication used for snapshot feed
    private final Aeron aeron;
    private final Publication snapshotPublication;

    // We publish snapshot every 60s in background
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "md-snap-publisher")
    );

    private volatile boolean running = true;

    public MarketDataSnapshotPublisher(LFQueue<MarketUpdate> marketUpdateLFQueue,
                                       int maxTickers,
                                       String snapshotChannel,
                                       int snapshotStreamId) {
        this.marketUpdateLFQueue = marketUpdateLFQueue;
        this.marketUpdateLFQueue.subscribe(this::onIncrementalUpdate);

        // Pre-allocate data structure for snapshot
        this.tickerOrders = new ArrayList<>(maxTickers);
        for (int i = 0; i < maxTickers; i++) {
            this.tickerOrders.add(new HashMap<>());
        }

        // Launch an embedded MediaDriver or connect to external one
        MediaDriver.Context mediaCtx = new MediaDriver.Context();
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaCtx);
        Aeron.Context aeronCtx = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());
        this.aeron = Aeron.connect(aeronCtx);

        // Create a separate publication for snapshot feed
        this.snapshotPublication = aeron.addPublication(snapshotChannel, snapshotStreamId);

        // Schedule a snapshot publication every 60s
        scheduler.scheduleAtFixedRate(
                this::publishSnapshot,
                60, // initial delay
                60, // repeat interval
                TimeUnit.SECONDS
        );

        log.info("MarketDataSnapshotPublisher started. Snapshots go to {}:{} every 60s", snapshotChannel, snapshotStreamId);
    }

    /**
     * Called upon new MarketUpdate from incremental feed queue.
     */
    private void onIncrementalUpdate(MarketUpdate incrementalUpdate) {
        if (incrementalUpdate == null) {
            log.warn("Null MarketUpdate received");
            return;
        }
        long seq = incrementalUpdate.getSeqNum();
        if (seq != lastIncSeqNum + 1) {
            log.warn("Out of order seqNum: got {}, expected {}", seq, lastIncSeqNum + 1);
        }
        lastIncSeqNum = seq;

        long tickerId = incrementalUpdate.getTickerId();
        long orderId = incrementalUpdate.getOrderId();
        MarketUpdateType type = incrementalUpdate.getType();

        Map<Long, MarketUpdate> ordersForTicker = tickerOrders.get((int) tickerId);

        switch (type) {
            case ADD: {
                if (ordersForTicker.containsKey(orderId)) {
                    log.error("ADD for existing orderId={} tickerId={}", orderId, tickerId);
                } else {
                    MarketUpdate newOrder = copyOf(incrementalUpdate);
                    ordersForTicker.put(orderId, newOrder);
                }
                break;
            }
            case MODIFY: {
                MarketUpdate existing = ordersForTicker.get(orderId);
                if (existing == null) {
                    log.error("MODIFY for non-existing orderId={} tickerId={}", orderId, tickerId);
                } else {
                    existing.setQty(incrementalUpdate.getQty());
                    existing.setPrice(incrementalUpdate.getPrice());
                }
                break;
            }
            case CANCEL: {
                MarketUpdate existing = ordersForTicker.remove(orderId);
                if (existing == null) {
                    log.error("CANCEL for non-existing orderId={} tickerId={}", orderId, tickerId);
                }
                break;
            }
            case TRADE:
            case CLEAR:
            case SNAPSHOT_START:
            case SNAPSHOT_END:
            case INVALID:
                break;
        }
    }

    /**
     * Publish the entire snapshot to snapshotPublication.
     * We'll send:
     *  1) SNAPSHOT_START with orderId=lastIncSeqNum
     *  2) CLEAR + each order
     *  3) SNAPSHOT_END with orderId=lastIncSeqNum
     */
    public void publishSnapshot() {
        if (!running) {
            return;
        }
        long snapshotSeq = 0;
        long incSeqUsed = lastIncSeqNum; // the last incremental seq used

        log.info("== Publishing snapshot start, lastIncSeqNum={} ==", incSeqUsed);

        // 1) SNAPSHOT_START
        MarketUpdate startMsg = new MarketUpdate();
        startMsg.setSeqNum(snapshotSeq++);
        startMsg.setType(MarketUpdateType.SNAPSHOT_START);
        startMsg.setOrderId(incSeqUsed);
        publishToAeron(startMsg);

        // 2) For each ticker: send CLEAR, then each order
        for (int t = 0; t < tickerOrders.size(); t++) {
            // CLEAR
            MarketUpdate clearMsg = new MarketUpdate();
            clearMsg.setSeqNum(snapshotSeq++);
            clearMsg.setType(MarketUpdateType.CLEAR);
            clearMsg.setTickerId(t);
            publishToAeron(clearMsg);

            // Then each order
            Map<Long, MarketUpdate> ordersMap = tickerOrders.get(t);
            for (MarketUpdate order : ordersMap.values()) {
                // We copy so we can rewrite seqNum in snapshot
                MarketUpdate orderCopy = copyOf(order);
                orderCopy.setSeqNum(snapshotSeq++);
                publishToAeron(orderCopy);
            }
        }

        // 3) SNAPSHOT_END
        MarketUpdate endMsg = new MarketUpdate();
        endMsg.setSeqNum(snapshotSeq++);
        endMsg.setType(MarketUpdateType.SNAPSHOT_END);
        endMsg.setOrderId(incSeqUsed);
        publishToAeron(endMsg);

        log.info("== Publishing snapshot end, total msgs={} ==", snapshotSeq);
    }

    /**
     * Encode the MarketUpdate in a simple binary format, then offer to snapshotPublication.
     */
    private void publishToAeron(MarketUpdate mu) {
        // Minimal example: seqNum(8), type(1), side(1), orderId(8), tickerId(8), price(8), qty(8), priority(8)
        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
        int offset = 0;

        buffer.putLong(offset, mu.getSeqNum(), ByteOrder.LITTLE_ENDIAN);
        offset += 8;

        buffer.putByte(offset, (byte) mu.getType().ordinal());
        offset += 1;

        buffer.putByte(offset, (byte) mu.getSide().ordinal());
        offset += 1;

        buffer.putLong(offset, mu.getOrderId(), ByteOrder.LITTLE_ENDIAN);
        offset += 8;

        buffer.putLong(offset, mu.getTickerId(), ByteOrder.LITTLE_ENDIAN);
        offset += 8;

        buffer.putLong(offset, mu.getPrice(), ByteOrder.LITTLE_ENDIAN);
        offset += 8;

        buffer.putLong(offset, mu.getQty(), ByteOrder.LITTLE_ENDIAN);
        offset += 8;

        buffer.putLong(offset, mu.getPriority(), ByteOrder.LITTLE_ENDIAN);
        offset += 8;

        long result;
        do {
            result = snapshotPublication.offer(buffer, 0, offset);
            if (result < 0) {
                if (result == Publication.NOT_CONNECTED || result == Publication.BACK_PRESSURED) {
                    Thread.yield();
                } else {
                    log.warn("Publication error: {} for marketUpdate {}", result, mu);
                    break;
                }
            }
        } while (result < 0);

        // For debugging
        log.debug("SnapshotPub: sent msg seqNum={} type={}", mu.getSeqNum(), mu.getType());
    }

    /**
     * Make a copy of an existing MarketUpdate, since we might want to rewrite seqNum for snapshot
     */
    private MarketUpdate copyOf(MarketUpdate src) {
        MarketUpdate copy = new MarketUpdate();
        copy.setSeqNum(src.getSeqNum());
        copy.setType(src.getType());
        copy.setOrderId(src.getOrderId());
        copy.setTickerId(src.getTickerId());
        copy.setSide(src.getSide());
        copy.setPrice(src.getPrice());
        copy.setQty(src.getQty());
        copy.setPriority(src.getPriority());
        return copy;
    }

    /**
     * Shut down the scheduler, mark no longer running
     */
    public void close() {
        running = false;
        scheduler.shutdown();
        // Optionally close snapshotPublication, Aeron, etc.
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        snapshotPublication.close();
        aeron.close();
        log.info("MarketDataSnapshotPublisher closed.");
    }

}