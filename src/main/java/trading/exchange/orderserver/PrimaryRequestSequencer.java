package trading.exchange.orderserver;

import aeron.AeronConsumer;
import aeron.ArchivePublisher;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.common.LFQueue;
import trading.common.Utils;

public final class PrimaryRequestSequencer extends AbstractRequestSequencer {
    private static final Logger log = LoggerFactory.getLogger(PrimaryRequestSequencer.class);

    private static final int POOL_CAPACITY = 1 << 22;        // 4M

    private final AeronConsumer replicationAckConsumer;

    private final OneToOneConcurrentArrayQueue<OrderRequest> freePool =
            new OneToOneConcurrentArrayQueue<>(POOL_CAPACITY);

    private final OneToOneConcurrentArrayQueue<OrderRequest> inFlight =
            new OneToOneConcurrentArrayQueue<>(POOL_CAPACITY);

    public PrimaryRequestSequencer(LFQueue<OrderRequest> clientRequests, long lastSeqNum) {
        super(clientRequests, lastSeqNum);

        for (int i = 0; i < POOL_CAPACITY; i++) {
            freePool.offer(new OrderRequest());
        }

        String aeronIp = Utils.env("AERON_IP", "224.0.1.1");
        int ackPort = Integer.parseInt(Utils.env("REPLICATION_ACK_PORT", "40552"));
        int ackStream = Integer.parseInt(Utils.env("REPLICATION_ACK_STREAM", "3002"));

        FragmentHandler fh = (buf, off, len, header) -> onAck(buf.getLong(off));
        this.replicationAckConsumer =
                new AeronConsumer(aeronIp, String.valueOf(ackPort), ackStream, fh, "REPLICATION_ACK");
    }

    @Override
    public void process(final OrderRequest src) {
        OrderRequest dst = freePool.poll();
        if (dst == null) {
            dst = new OrderRequest(); /*Should almost never happen */
            log.error("freePool. EMPTY while processing {}", src);
        }

        OrderRequest.copy(src, dst);
        dst.setSeqNum(getNextSeqNum());

        replicate(dst);

        while (!inFlight.offer(dst)) {
            Thread.onSpinWait();
        }
    }

    private void onAck(final long seq) {
        for (; ; ) {
            OrderRequest req = inFlight.poll();
            if (req == null) {
                log.info("onAck. Ack dup {}", seq);
                if (log.isDebugEnabled()) {
                    log.debug("onAck. Ack dup {}", seq);
                }
                return;
            }

            long msgSeq = req.getSeqNum();
            if (msgSeq == seq) {
                passToEngine(req);
                recycle(req);
                return;
            }

            if (msgSeq < seq) {
                log.debug("Ack dup: {}, {}", msgSeq, req);
                recycle(req);
                continue;
            }

            /* Should not happen */
            log.error("ACK out of order: ack={} head={}", seq, msgSeq);
            inFlight.offer(req);
            return;
        }
    }

    private void recycle(final OrderRequest r) {
        r.reset();
        if (!freePool.offer(r)) {
            log.error("Pool is full. Dropping {}", r);
        }
    }

    @Override
    public void run() {
        log.info("starting");
        super.run();
        replicationAckConsumer.run();
        log.info("started");

    }

    @Override
    public void shutdown() {
        log.info("stopping");
        super.shutdown();
        replicationAckConsumer.stop();
        log.info("stopped");
    }

}