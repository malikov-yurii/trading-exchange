package trading.participant.marketdata;

import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.MarketUpdateSerDe;
import aeron.AeronConsumer;
import trading.common.LFQueue;
import trading.common.Utils;
import trading.participant.strategy.TradeEngineUpdate;

public class MarketDataConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MarketDataConsumer.class);

    private final LFQueue<TradeEngineUpdate> tradeEngineUpdates;
    private final AeronConsumer aeronConsumer;
    private final TradeEngineUpdate tradeEngineUpdate = new TradeEngineUpdate();

    public MarketDataConsumer(LFQueue<TradeEngineUpdate> tradeEngineUpdates) {
        String mdIp = Utils.env("AERON_IP", "224.0.1.1");
        String mdPort = Utils.env("MD_PORT", "40456");
        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> processBuffer(buffer, offset, length);
        this.aeronConsumer = new AeronConsumer(mdIp, mdPort, 1001, fragmentHandler, "MD");
        this.tradeEngineUpdates = tradeEngineUpdates;
    }

    @Override
    public void run() {
        log.info("MarketDataConsumer starting");
        aeronConsumer.run();
        log.info("MarketDataConsumer shutting down");
    }

    private void processBuffer(DirectBuffer buffer, int offset, int length) {
        MarketUpdate marketUpdate = MarketUpdateSerDe.deserialize(buffer, offset);
        if (log.isDebugEnabled()) {
//            log.debug("Received {}", marketUpdate);
        }
        log.info("Received {}", marketUpdate);
        //todo: FIX bug in trade engine. Stuck when uncommented
//        tradeEngineUpdate.set(marketUpdate);
//        tradeEngineUpdates.offer(tradeEngineUpdate);
    }

    public void shutdown() {
        log.info("Shutting down MarketDataConsumer");
        aeronConsumer.stop();
    }

}