package trading.exchange.marketdata;

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.MarketUpdateSerDe;
import trading.api.MarketUpdateType;
import aeron.AeronPublisher;
import trading.common.LFQueue;
import trading.common.ObjectPool;
import trading.common.Utils;
import trading.exchange.AppState;
import trading.exchange.LeadershipManager;
import trading.exchange.matching.Order;

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

    private final AppState appState;

    private final AeronPublisher aeronPublisher;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "md-snap-publisher")
    );

    private final ObjectPool<MarketUpdate> marketUpdatePool =
            new ObjectPool<>(30_000, MarketUpdate::new);

    private final ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);

    private volatile boolean running = true;

    public MarketDataSnapshotPublisher(LFQueue<MarketUpdate> marketUpdateLFQueue,
                                       AppState appState,
                                       int maxTickers) {

        this.marketUpdateLFQueue = marketUpdateLFQueue;
        this.appState = appState;
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

        int snapshotInterval = 600; // TODO: make this configurable
        scheduler.scheduleAtFixedRate(
                this::publishSnapshot,
                600, // initial delay
                snapshotInterval, // repeat interval
                TimeUnit.SECONDS
        );
        log.info("MarketDataSnapshotPublisher started. Snapshots go to {}:{} every {}s",
                channel, streamId, snapshotInterval);
    }

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
                    MarketUpdate newOrder = marketUpdatePool.acquire();
//                    MarketUpdate newOrder = new MarketUpdate();
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
//        MarketUpdate startMsg = marketUpdatePool.acquire(); //TODO here and other places
        startMsg.setSeqNum(snapshotSeq++);
        startMsg.setType(MarketUpdateType.SNAPSHOT_START);
        startMsg.setOrderId(incSeqUsed);
        publish(startMsg);

        // 2) For each ticker: send CLEAR, then each order
        for (int t = 0; t < tickerOrders.size(); t++) {
            // CLEAR
//            MarketUpdate clearMsg = marketUpdatePool.acquire();
            MarketUpdate clearMsg = new MarketUpdate();
            clearMsg.setSeqNum(snapshotSeq++);
            clearMsg.setType(MarketUpdateType.CLEAR);
            clearMsg.setTickerId(t);
            publish(clearMsg);

            // Then each order
            Map<Long, MarketUpdate> ordersMap = tickerOrders.get(t);
            for (MarketUpdate order : ordersMap.values()) {
                // We copy so we can rewrite seqNum in snapshot
//                MarketUpdate orderCopy = marketUpdatePool.acquire();
                MarketUpdate orderCopy = new MarketUpdate();
                MarketUpdate.copy(order, orderCopy);
                orderCopy.setSeqNum(snapshotSeq++);
                publish(orderCopy);
            }
        }

        // 3) SNAPSHOT_END
//        MarketUpdate endMsg = marketUpdatePool.acquire();
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

        if (appState.isNotRecoveredLeader()) {
            log.debug("Not Publishing {}", marketUpdate);
            return;
        }

        int offset = 0;
        int length = MarketUpdateSerDe.serializeMarketUpdate(marketUpdate, buffer, offset);

        aeronPublisher.publish(buffer, offset, length);
        log.info("Published {}", marketUpdate);
        marketUpdatePool.release(marketUpdate);
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