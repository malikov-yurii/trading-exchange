package trading.exchange.orderserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.common.LFQueue;

public final class BackupRequestSequencer extends AbstractRequestSequencer {
    private static final Logger log = LoggerFactory.getLogger(BackupRequestSequencer.class);

    public BackupRequestSequencer(LFQueue<OrderRequest> clientRequests, long lastSeqNum) {
        super(clientRequests, lastSeqNum);
        log.info("initialised");
    }

    @Override
    public void process(final OrderRequest orderRequest) {
        orderRequest.setSeqNum(getNextSeqNum());

        replicate(orderRequest);

        passToEngine(orderRequest);
    }

}