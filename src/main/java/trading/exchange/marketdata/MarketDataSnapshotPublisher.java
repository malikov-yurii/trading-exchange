package trading.exchange.marketdata;

import aeron.AeronPublisher;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.MarketUpdateSerDe;
import trading.api.MarketUpdateType;
import trading.common.LFQueue;
import trading.common.ObjectPool;
import trading.common.SingleThreadRingBuffer;
import trading.exchange.AppState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static trading.common.Utils.env;

public class MarketDataSnapshotPublisher {

    private static final Logger log = LoggerFactory.getLogger(MarketDataSnapshotPublisher.class);

    private final List<Map<Long, MarketUpdate>> tickerOrders;
    private final AppState appState;
    private final AeronPublisher aeronPublisher;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "md-snap-publisher")
    );

    private final ObjectPool<MarketUpdate> marketUpdatePool = new ObjectPool<>(30_000, MarketUpdate::new,
            size -> new SingleThreadRingBuffer<>(size, "md-snap-pool", false));
    private final ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);
    private final MarketUpdate snapshotMsg = new MarketUpdate();

    private volatile long lastIncSeqNum = -1L;
    private volatile boolean running = true;

    public MarketDataSnapshotPublisher(LFQueue<MarketUpdate> marketUpdateLFQueue,
                                       AppState appState, int maxTickers) {
        this.appState = appState;
        marketUpdateLFQueue.subscribe(this::onIncrementalUpdate);

        this.tickerOrders = new ArrayList<>(maxTickers);
        for (int i = 0; i < maxTickers; i++) {
            this.tickerOrders.add(new HashMap<>());
        }

        String mdIp = env("AERON_IP", "224.0.1.1");
        String mdSnapshotPort = env("MD_SNAPSHOT_PORT", "40457");
        String channel = "aeron:udp?endpoint=" + mdIp + ":" + mdSnapshotPort;
        int streamId = 2001;
        this.aeronPublisher = new AeronPublisher(channel, streamId, "MD-SNAPSHOT");


        int snapshotInterval = Integer.parseInt(env("MD_SNAPSHOT_INTERVAL", "180"));
        int initialDelay = Integer.parseInt(env("MD_SNAPSHOT_INITIAL_DELAY", "60"));
        scheduler.scheduleAtFixedRate(
                this::publishSnapshot,
                initialDelay, // initial delay
                snapshotInterval, // repeat interval
                TimeUnit.SECONDS
        );
        log.info("MarketDataSnapshotPublisher started. Initial delay {}s. Snapshots go to {}:{} every {}s",
                initialDelay, channel, streamId, snapshotInterval);
    }

    private synchronized void onIncrementalUpdate(MarketUpdate incrementalUpdate) {
        try {
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
                        MarketUpdate newOrder = marketUpdatePool.acquire();
                        MarketUpdate.copy(incrementalUpdate, newOrder);
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
                    } else {
                        marketUpdatePool.release(existing);
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
        } catch (Exception exception) {
            log.error("onIncrementalUpdate", exception);
        }
    }

    /**
     * Publish the entire snapshot to snapshotPublication.
     * We'll send:
     * 1) SNAPSHOT_START with orderId=lastIncSeqNum
     * 2) CLEAR + each order
     * 3) SNAPSHOT_END with orderId=lastIncSeqNum
     */
    public synchronized void publishSnapshot() {
        if (!running) {
            return;
        }

        long snapshotSeq = 0;
        long incSeqUsed = lastIncSeqNum; // the last incremental seq used

        if (appState.isRecoveredLeader()) {
            log.info("== Publishing snapshot start, lastIncSeqNum={} ==", incSeqUsed);
        } else if (log.isDebugEnabled()) {
            log.debug("== Not Publishing snapshot start, lastIncSeqNum={} ==", incSeqUsed);
        }

        // 1) SNAPSHOT_START
        snapshotMsg.reset();
        snapshotMsg.setSeqNum(snapshotSeq++);
        snapshotMsg.setType(MarketUpdateType.SNAPSHOT_START);
        snapshotMsg.setOrderId(incSeqUsed);
        publish(snapshotMsg);

        // 2) For each ticker: send CLEAR, then each order
        for (int t = 0; t < tickerOrders.size(); t++) {
            // CLEAR
            snapshotMsg.reset();
            snapshotMsg.setSeqNum(snapshotSeq++);
            snapshotMsg.setType(MarketUpdateType.CLEAR);
            snapshotMsg.setTickerId(t);
            publish(snapshotMsg);

            // Then each order
            Map<Long, MarketUpdate> ordersMap = tickerOrders.get(t);
            for (MarketUpdate order : ordersMap.values()) {
                // We copy so we can rewrite seqNum in snapshot
                MarketUpdate.copy(order, snapshotMsg);
                snapshotMsg.setSeqNum(snapshotSeq++);
                publish(snapshotMsg);
            }
        }

        // 3) SNAPSHOT_END
        snapshotMsg.reset();
        snapshotMsg.setSeqNum(snapshotSeq++);
        snapshotMsg.setType(MarketUpdateType.SNAPSHOT_END);
        snapshotMsg.setOrderId(incSeqUsed);
        publish(snapshotMsg);

        log.info("== Publishing snapshot end, total msgs={} ==", snapshotSeq);
    }


    private void publish(MarketUpdate marketUpdate) {
        if (marketUpdate == null) {
            log.warn("Null MarketUpdate received");
            return;
        }

        if (appState.isNotRecoveredLeader()) {
            if (log.isDebugEnabled()) {
                log.debug("Not Publishing {}", marketUpdate);
            }
            return;
        }

        int offset = 0;
        int length = MarketUpdateSerDe.serializeMarketUpdate(marketUpdate, buffer, offset);

        aeronPublisher.publish(buffer, offset, length);
        log.info("Published {}", marketUpdate);
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