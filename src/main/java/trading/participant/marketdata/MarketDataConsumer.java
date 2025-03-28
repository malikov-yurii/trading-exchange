package trading.participant.marketdata;

import trading.api.MarketUpdate;
import trading.api.MarketUpdateType;
import trading.api.Side;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.common.LFQueue;
import trading.participant.strategy.TradeEngineUpdate;

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

public class MarketDataConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MarketDataConsumer.class);

    // For example, same channel and stream as the publisher
    private static final String DEFAULT_CHANNEL = "aeron:udp?endpoint=224.0.1.1:40456";
    private static final int DEFAULT_STREAM_ID = 1001;

    private final Subscription subscription;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final LFQueue<TradeEngineUpdate> tradeEngineUpdates;

    public MarketDataConsumer(LFQueue<TradeEngineUpdate> tradeEngineUpdates) {
        // In real usage, connect to existing MediaDriver or embed again
        MediaDriver.Context mediaCtx = new MediaDriver.Context();
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaCtx);

        Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        Aeron aeron = Aeron.connect(aeronCtx);

        // Create a subscription to the same channel + stream
        this.subscription = aeron.addSubscription(DEFAULT_CHANNEL, DEFAULT_STREAM_ID);

        this.tradeEngineUpdates = tradeEngineUpdates;
    }

    /**
     * We run a loop poll()ing the subscription for new MarketUpdates.
     */
    @Override
    public void run() {
        log.info("MarketDataConsumer starting");
        // Simple busy-spin
        BusySpinIdleStrategy idle = new BusySpinIdleStrategy();
        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            processBuffer(buffer, offset, length);
        };

        while (running.get()) {
            int fragmentsRead = subscription.poll(fragmentHandler, 10);
            idle.idle(fragmentsRead);
        }
        log.info("MarketDataConsumer shutting down");
    }

    private void processBuffer(DirectBuffer buffer, int offset, int length) {
        // The publisher wrote data in little-endian, so interpret accordingly
        // We must parse the same fields we wrote: seqNum, type ordinal, side ordinal, etc.
        // This is a minimal example; you'd want more robust error-checking.

        int start = offset;
        long seqNum = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        byte typeOrd = buffer.getByte(start++);
        MarketUpdateType type = MarketUpdateType.values()[typeOrd];

        byte sideOrd = buffer.getByte(start++);
        Side side = Side.values()[sideOrd];

        long orderId = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        long tickerId = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        long price = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        long qty = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        long priority = buffer.getLong(start, ByteOrder.LITTLE_ENDIAN);
        start += Long.BYTES;

        MarketUpdate marketUpdate = new MarketUpdate(
                seqNum, type, orderId, tickerId, side, price, qty, priority);

//        log.info("Received {}", marketUpdate);
        tradeEngineUpdates.offer(new TradeEngineUpdate(marketUpdate, null));
    }

    public void shutdown() {
        running.set(false);
    }
}