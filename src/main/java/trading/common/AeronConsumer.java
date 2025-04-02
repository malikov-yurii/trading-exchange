package trading.common;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static trading.common.Utils.env;

public class AeronConsumer {
    private static final Logger log = LoggerFactory.getLogger(AeronConsumer.class);

    private final String name;
    private final Subscription subscription;
    private final FragmentHandler fragmentHandler;
    private final int sleepPeriodMs = 10;
    private final String channel;
    private final int streamId;

    private volatile boolean running;

    public AeronConsumer(String ip, String port, int streamId, FragmentHandler fragmentHandler, String name) {
        this.name = name;
        this.fragmentHandler = fragmentHandler;

        channel = "aeron:udp?endpoint=" + ip + ":" + port;
        this.streamId = streamId;
        this.subscription = AeronClient.INSTANCE.addSubscription(channel, this.streamId);
    }

    public void run() {
        String idle = env("MD_WAIT_STRATEGY", "SLEEPING_WAIT");
        IdleStrategy idleStrategy;
        String msg = String.format("--------------------> [%s] Starting AeronConsumer: channel: %s, streamId: %s. Idle Strategy %s. aeronDir: %s",
                name, channel, streamId, idle, AeronClient.getAeronDirectory());
        if ("SLEEPING_WAIT".equals(idle)) {
            msg += ". sleepPeriodMs=" + sleepPeriodMs;
            idleStrategy = new SleepingMillisIdleStrategy(sleepPeriodMs);
        } else {
            idleStrategy = new BusySpinIdleStrategy();
        }
        log.info(msg);

        running = true;
        while (running) {
            int fragmentsRead = subscription.poll(fragmentHandler, 10);
            idleStrategy.idle(fragmentsRead);
        }
        log.info("[{}] Shutting down", name);
    }

    public void stop() {
        running = false;
    }

}
