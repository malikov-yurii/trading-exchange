package trading.exchange.orderserver;

import aeron.AeronConsumer;
import fix.FixUtils;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trading.api.OrderRequest;
import trading.api.OrderRequestSerDe;

import java.util.function.Consumer;

import static aeron.AeronUtils.aeronIp;
import static trading.common.Utils.env;

public class IngressConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(IngressConsumer.class);

    private final AeronConsumer aeronConsumer;

    private final Consumer<OrderRequest> orderRequestConsumer;
    private Thread thread;

    public IngressConsumer(Consumer<OrderRequest> orderRequestConsumer) {
        this.orderRequestConsumer = orderRequestConsumer;

        this.aeronConsumer =
                new AeronConsumer(aeronIp(), env("INGRESS_PORT", "40553"),
                        Integer.parseInt(env("INGRESS_STREAM", "3003")),
                        this::processOrderRequest, "INGRESS");
    }

    private void processOrderRequest(DirectBuffer buffer, int offset, int length, Header header) {
        OrderRequest orderRequest = OrderRequestSerDe.deserializeClientRequest(buffer, offset, length);
        if (log.isDebugEnabled()) {
            log.debug("{} Received {} offset {} length {}", FixUtils.getTestTag(orderRequest.getOrderId()), orderRequest, offset, length);
        }

        orderRequestConsumer.accept(orderRequest);
    }

    public void shutdown() {
        log.info("Shutdown");
        aeronConsumer.stop();
        if (thread != null) {
            thread.interrupt();
        }
    }

    public void start() {
        thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {
        log.info("Starting");
        aeronConsumer.run();
    }

}
