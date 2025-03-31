package trading.participant.marketdata;

import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.MarketUpdate;
import trading.api.MarketUpdateSerDe;
import trading.common.AeronConsumer;
import trading.common.Utils;

public class MarketDataSnapshotConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MarketDataSnapshotConsumer.class);

    private final AeronConsumer aeronConsumer;

    public MarketDataSnapshotConsumer() {
        String mdIp = Utils.env("AERON_IP", "224.0.1.1");
        String mdPort = Utils.env("MD_SNAPSHOT_PORT", "40457");
        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> processBuffer(buffer, offset, length);
        this.aeronConsumer = new AeronConsumer(mdIp, mdPort, 2001, fragmentHandler, "MD");
    }

    @Override
    public void run() {
        log.info("MarketDataSnapshotConsumer starting");
        aeronConsumer.stop();
        log.info("MarketDataSnapshotConsumer shutting down");
    }

    private void processBuffer(DirectBuffer buffer, int offset, int length) {
        MarketUpdate marketUpdate = MarketUpdateSerDe.deserialize(buffer, offset);
        if (log.isDebugEnabled()) {
            log.debug("Received {}", marketUpdate);
        }
    }

    public void shutdown() {
        log.info("Shutting down MarketDataSnapshotConsumer");
        aeronConsumer.stop();
    }
}