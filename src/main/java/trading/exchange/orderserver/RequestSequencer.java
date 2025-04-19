package trading.exchange.orderserver;

import aeron.AeronConsumer;
import aeron.ArchivePublisher;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.LFQueue;
import trading.common.Utils;
import trading.exchange.AppState;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Single‑producer  (process thread)
 * Single‑consumer  (ACK handler thread)
 */
public final class RequestSequencer implements Runnable {
    /* ---------- config --------------------------------------------------- */
    private static final Logger log = LoggerFactory.getLogger(RequestSequencer.class);

    private static final int   POOL_CAPACITY   = 1 << 22;        // 4M

    /* ---------- dependencies --------------------------------------------- */
    private final LFQueue<OrderRequest> clientRequests;
    private final ArchivePublisher      replicationPublisher;
    private final AeronConsumer         replicationAckConsumer;

    /* ---------- concurrent structures ------------------------------------ */
    /** free object pool     (consumer → producer) */
    private final OneToOneConcurrentArrayQueue<OrderRequest> freePool =
            new OneToOneConcurrentArrayQueue<>(POOL_CAPACITY);

    /** in‑flight tracking   (producer → consumer) */
    private final OneToOneConcurrentArrayQueue<OrderRequest> inFlight =
            new OneToOneConcurrentArrayQueue<>(POOL_CAPACITY);

    /* ---------- bookkeeping ---------------------------------------------- */
    private final AtomicLong nextSeq;
//    private final Thread     ownerThread;        // for misuse checks

    /* ---------- helpers --------------------------------------------------- */
    private static final ThreadLocal<ExpandableDirectByteBuffer> BUF =
            ThreadLocal.withInitial(() -> new ExpandableDirectByteBuffer(128));

    /* ===================================================================== */
    public RequestSequencer(LFQueue<OrderRequest> clientRequests,
                            long                     lastSeqNum,
                            AppState                 appState) {

        this.clientRequests = clientRequests;
        this.nextSeq        = new AtomicLong(lastSeqNum + 1);
//        this.ownerThread    = Thread.currentThread();

        // pre‑allocate objects into the free pool
        for (int i = 0; i < POOL_CAPACITY; i++) {
            freePool.offer(new OrderRequest());
        }

        // ------------- replication publisher ------------------------------
        final int replicationStream = Integer.parseInt(Utils.env("REPLICATION_STREAM","3001"));
        this.replicationPublisher   = new ArchivePublisher(replicationStream, "REPLICATION", false);

        // ------------- replication ACK consumer ---------------------------
        String aeronIp   = Utils.env("AERON_IP", "224.0.1.1");
        int    ackPort   = Integer.parseInt(Utils.env("REPLICATION_ACK_PORT",   "40552"));
        int    ackStream = Integer.parseInt(Utils.env("REPLICATION_ACK_STREAM", "3002"));

        FragmentHandler fh = (buf, off, len, header) -> onAck(buf.getLong(off));

        this.replicationAckConsumer =
                new AeronConsumer(aeronIp, String.valueOf(ackPort), ackStream, fh, "REPLICATION_ACK");

        log.info("RequestSequencer initialised: pool={}, stream={}", POOL_CAPACITY, replicationStream);
    }

    /* ===================================================================== */
    /** Called only by the *producer* thread. */
    public void process(final OrderRequest src) {
//        assertOwner();

        OrderRequest dst = freePool.poll();
        if (dst == null){
            dst = new OrderRequest();        // extremely rare
            log.error("freePool. EMPTY while processing {}", src);
        }

        OrderRequest.copy(src, dst);
        dst.setSeqNum(nextSeq.getAndIncrement());

        /* replicate ------------------------------------------------------- */
        var buf = BUF.get();
        int len = OrderRequestSerDe.serialize(dst, buf, 0);
        replicationPublisher.publish(buf, 0, len);

        /* track in‑flight -------------------------------------------------- */
        while (!inFlight.offer(dst)) {
            Thread.onSpinWait();
        }
    }

    /* ---------- ACK path (single consumer thread) ------------------------ */
    private void onAck(final long seq) {
        for (;;) {
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
                clientRequests.offer(req);
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

    /* ---------- pool helpers -------------------------------------------- */
    private void recycle(final OrderRequest r) {
        r.reset();                                   // clear fields incl. seq
        if (!freePool.offer(r)) {
            /* shouldn't happen, but drop if pool is unexpectedly full      */
        }
    }

    /* ---------- life‑cycle ---------------------------------------------- */
    public void shutdown() {
        replicationPublisher.close();
        replicationAckConsumer.stop();
    }

    @Override public void run() {
        log.info("RequestSequencer starting");
        replicationAckConsumer.run();               // blocks
        log.info("RequestSequencer stopped");
    }

//    /* ---------- misc ----------------------------------------------------- */
//    private void assertOwner() {
//        if (Thread.currentThread() != ownerThread) {
//            throw new IllegalStateException("process() called by multiple threads");
//        }
//    }
}