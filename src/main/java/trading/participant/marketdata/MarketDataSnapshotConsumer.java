package trading.participant.marketdata;

import com.aitusoftware.babl.config.PerformanceMode;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
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

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

import static trading.common.Utils.env;

public class MarketDataSnapshotConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MarketDataSnapshotConsumer.class);

    // For example, same channel and stream as the publisher
    private static final String DEFAULT_CHANNEL = "aeron:udp?endpoint=224.0.1.1:40457";
    private static final int DEFAULT_STREAM_ID = 2001;

    private final Subscription subscription;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public MarketDataSnapshotConsumer() {
        this(DEFAULT_CHANNEL, DEFAULT_STREAM_ID);
    }

    public MarketDataSnapshotConsumer(String channel, int streamId) {
        // In real usage, connect to existing MediaDriver or embed again
        MediaDriver.Context mediaCtx = new MediaDriver.Context();
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaCtx);

        Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        Aeron aeron = Aeron.connect(aeronCtx);

        // Create a subscription to the same channel + stream
        this.subscription = aeron.addSubscription(channel, streamId);
    }

    /**
     * We run a loop poll()ing the subscription for new MarketUpdates.
     */
    @Override
    public void run() {
        log.info("MarketDataSnapshotConsumer starting");

        IdleStrategy idle;
        String mdPerfMode = env("MD_WAIT_STRATEGY", "SLEEPING_WAIT");
        if ("SLEEPING_WAIT".equals(mdPerfMode)) {
            int sleepPeriodMs = 10;
            idle = new SleepingMillisIdleStrategy(sleepPeriodMs);
            log.info("Using SleepingMillisIdleStrategy. sleepPeriodMs={}", sleepPeriodMs);
        } else {
            idle = new BusySpinIdleStrategy();
        }

        FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
            processBuffer(buffer, offset, length);
        };

        while (running.get()) {
            int fragmentsRead = subscription.poll(fragmentHandler, 10);
            idle.idle(fragmentsRead);
        }
        log.info("MarketDataSnapshotConsumer shutting down");
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

        // Log the MarketUpdate
        log.info("Received {}", marketUpdate);
    }

    public void shutdown() {
        running.set(false);
    }
}