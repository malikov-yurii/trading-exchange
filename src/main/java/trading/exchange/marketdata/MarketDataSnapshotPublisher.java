package trading.exchange.marketdata;

import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.MarketUpdateSerDe;
import trading.api.MarketUpdateType;
import trading.common.AeronPublisher;
import trading.common.LFQueue;
import trading.common.Utils;
import trading.exchange.LeadershipManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MarketDataSnapshotPublisher {

    private static final Logger log = LoggerFactory.getLogger(MarketDataSnapshotPublisher.class);

    private final List<Map<Long, MarketUpdate>> tickerOrders;

    private long lastIncSeqNum = -1L;

    private final LFQueue<MarketUpdate> marketUpdateLFQueue;

    private final LeadershipManager leadershipManager;

    private final AeronPublisher aeronPublisher;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "md-snap-publisher")
    );

    private volatile boolean running = true;

    public MarketDataSnapshotPublisher(LFQueue<MarketUpdate> marketUpdateLFQueue,
                                       LeadershipManager leadershipManager,
                                       int maxTickers) {

        this.marketUpdateLFQueue = marketUpdateLFQueue;
        this.leadershipManager = leadershipManager;
        this.marketUpdateLFQueue.subscribe(this::onIncrementalUpdate);

        this.tickerOrders = new ArrayList<>(maxTickers);
        for (int i = 0; i < maxTickers; i++) {
            this.tickerOrders.add(new HashMap<>());
        }

        String mdIp = Utils.env("AERON_IP", "224.0.1.1");
        String mdSnapshotPort = Utils.env("MD_SNAPSHOT_PORT", "40457");
        String channel = "aeron:udp?endpoint=" + mdIp + ":" + mdSnapshotPort;
        int streamId = 2001;
        this.aeronPublisher = new AeronPublisher(channel, streamId, "MD-SNAPSHOT");

        int repeatInterval = 60;
        scheduler.scheduleAtFixedRate(
                this::publishSnapshot,
                60, // initial delay
                repeatInterval, // repeat interval
                TimeUnit.SECONDS
        );
        log.info("MarketDataSnapshotPublisher started. Snapshots go to {}:{} every {}s",
                channel, streamId, repeatInterval);
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
     * 1) SNAPSHOT_START with orderId=lastIncSeqNum
     * 2) CLEAR + each order
     * 3) SNAPSHOT_END with orderId=lastIncSeqNum
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
        publish(startMsg);

        // 2) For each ticker: send CLEAR, then each order
        for (int t = 0; t < tickerOrders.size(); t++) {
            // CLEAR
            MarketUpdate clearMsg = new MarketUpdate();
            clearMsg.setSeqNum(snapshotSeq++);
            clearMsg.setType(MarketUpdateType.CLEAR);
            clearMsg.setTickerId(t);
            publish(clearMsg);

            // Then each order
            Map<Long, MarketUpdate> ordersMap = tickerOrders.get(t);
            for (MarketUpdate order : ordersMap.values()) {
                // We copy so we can rewrite seqNum in snapshot
                MarketUpdate orderCopy = copyOf(order);
                orderCopy.setSeqNum(snapshotSeq++);
                publish(orderCopy);
            }
        }

        // 3) SNAPSHOT_END
        MarketUpdate endMsg = new MarketUpdate();
        endMsg.setSeqNum(snapshotSeq++);
        endMsg.setType(MarketUpdateType.SNAPSHOT_END);
        endMsg.setOrderId(incSeqUsed);
        publish(endMsg);

        log.info("== Publishing snapshot end, total msgs={} ==", snapshotSeq);
    }


    private void publish(MarketUpdate marketUpdate) {
        if (marketUpdate == null) {
            log.warn("Null MarketUpdate received");
            return;
        }

        if (leadershipManager.isFollower()) {
            log.info("Not Published {}", marketUpdate);
            return;
        }

        ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
        int offset = 0;
        int length = MarketUpdateSerDe.serializeMarketUpdate(marketUpdate, buffer, offset);

        aeronPublisher.publish(buffer, offset, length);

        log.info("Published {}", marketUpdate);
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

    public void close() {
        running = false;
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        aeronPublisher.close();
        log.info("MarketDataSnapshotPublisher closed.");
    }

}