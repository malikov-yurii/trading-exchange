package trading.exchange.orderserver;

import aeron.ArchivePublisher;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;
import trading.common.LFQueue;
import trading.common.Utils;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractRequestSequencer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AbstractRequestSequencer.class);

    private final LFQueue<OrderRequest> clientRequests;

    private final AtomicLong nextSeq;

    private static final ThreadLocal<ExpandableDirectByteBuffer> BUF =
            ThreadLocal.withInitial(() -> new ExpandableDirectByteBuffer(128));
    private final IngressConsumer ingressConsumer;
    private final ArchivePublisher replicationPublisher;
    private Thread requestSequencerThread;

    public AbstractRequestSequencer(LFQueue<OrderRequest> clientRequests, long lastSeqNum) {

        this.clientRequests = clientRequests;
        this.nextSeq = new AtomicLong(lastSeqNum + 1);

        final int replicationStream = Integer.parseInt(Utils.env("REPLICATION_STREAM", "3001"));
        this.replicationPublisher = new ArchivePublisher(replicationStream, "REPLICATION", false);

        ingressConsumer = new IngressConsumer(this::process);
        log.info("initialised");
    }

    public abstract void process(final OrderRequest orderRequest);

    protected void passToEngine(OrderRequest orderRequest) {
        clientRequests.offer(orderRequest);
    }

    protected void replicate(OrderRequest dst) {
        var buf = BUF.get();
        int len = OrderRequestSerDe.serialize(dst, buf, 0);
        replicationPublisher.publish(buf, 0, len);
    }

    protected long getNextSeqNum() {
        return nextSeq.getAndIncrement();
    }

    public void start() {
        requestSequencerThread = new Thread(this, "RequestSequencerThread");
        requestSequencerThread.start();
    }

    @Override
    public void run() {
        try {
            log.info("starting");
            ingressConsumer.start();
            log.info("started");
        } catch (Exception ex) {
            log.error("run", ex);
        }
    }

    public void shutdown() {
        try {
            log.info("stopping");
            replicationPublisher.close();
            ingressConsumer.shutdown();
            if (requestSequencerThread != null && requestSequencerThread.isAlive()) {
                requestSequencerThread.interrupt();
                try {
                    requestSequencerThread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.info("stopped");
        } catch (Exception ex) {
            log.error("shutdown", ex);
        }

    }

}