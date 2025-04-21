package trading.exchange.marketdata;

import aeron.AeronPublisher;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import trading.api.MarketUpdate;
import trading.api.MarketUpdateSerDe;
import trading.common.AsyncLogger;
import trading.common.LFQueue;
import trading.common.Utils;
import trading.exchange.AppState;

import java.util.concurrent.atomic.AtomicLong;

public class MarketDataPublisher {
    private static final Logger log = LoggerFactory.getLogger(MarketDataPublisher.class);

    private final AsyncLogger asyncLogger;

    private final LFQueue<MarketUpdate> marketUpdateLFQueue;
    private final LFQueue<MarketUpdate> sequencedMarketUpdates;
    private final AeronPublisher aeronPublisher;
    private final AppState appState;
    ;
    private final AtomicLong msgSeqNum = new AtomicLong();
    private final ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(128);

    public MarketDataPublisher(LFQueue<MarketUpdate> marketUpdateLFQueue,
                               LFQueue<MarketUpdate> sequencedMarketUpdates,
                               AppState appState, AsyncLogger asyncLogger) {

        String mdIp = Utils.env("AERON_IP", "224.0.1.1");
        String mdPort = Utils.env("MD_PORT", "40456");
        String channel = "aeron:udp?endpoint=" + mdIp + ":" + mdPort;
        this.aeronPublisher = new AeronPublisher(channel, 1001, "MD");
        this.marketUpdateLFQueue = marketUpdateLFQueue;
        this.sequencedMarketUpdates = sequencedMarketUpdates;
        this.appState = appState;
        this.marketUpdateLFQueue.subscribe(this::publish);
        this.asyncLogger = asyncLogger;
        log.info("MarketDataPublisher initialized");
    }

    private void publish(MarketUpdate marketUpdate) {
        try {
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

            marketUpdate.setSeqNum(msgSeqNum.getAndIncrement());
            this.sequencedMarketUpdates.offer(marketUpdate);

            int offset = 0;
            int length = MarketUpdateSerDe.serializeMarketUpdate(marketUpdate, buffer, offset);

            aeronPublisher.publish(buffer, offset, length);

            asyncLogger.log("MD OUT", Level.INFO, "%s", marketUpdate);
//            log.info("Published {}", marketUpdate);
        } catch (Exception exception) {
            log.error("onIncrementalUpdate", exception);
        }
    }

    public void close() {
        aeronPublisher.close();
    }
}