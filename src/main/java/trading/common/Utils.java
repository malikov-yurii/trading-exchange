package trading.common;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);
    public static String env(String envVar, String defaultValue) {
        return ObjectUtils.defaultIfNull(System.getenv(envVar), defaultValue);
    }

    public static List<String> getOrderServerUris() {
        String orderServerHosts = env("ORDER_SERVER_HOSTS", null);
        int orderServerPort = Integer.parseInt(env("WS_PORT", null));

        if (StringUtils.isAnyBlank(orderServerHosts)) {
            log.error("ORDER_SERVER_HOSTS {} and WS_PORT {} env vars are required", orderServerHosts, orderServerPort);
            throw new IllegalArgumentException("ORDER_SERVER_HOSTS and WS_PORT env vars are required");
        }
        return Stream.of(orderServerHosts.split(","))
                .peek(host -> {
                    if (StringUtils.isBlank(host)) {
                        log.error("ORDER_SERVER_HOSTS env var is not valid");
                        throw new IllegalArgumentException("ORDER_SERVER_HOSTS env var is not valid");
                    }
                })
                .map(host -> "ws://" + host + ":" + orderServerPort + "/ws")
                .collect(Collectors.toList());
    }

    public static WaitStrategy getDisruptorWaitStrategy() {
        WaitStrategy waitStrategy;
        String env = env("DISRUPTOR_WAIT_STRATEGY", "SLEEPING_WAIT");
        if ("BLOCKING_WAIT".equals(env)) {
            waitStrategy = new BlockingWaitStrategy();
        } else if ("SLEEPING_WAIT".equals(env)) {
            waitStrategy = new SleepingWaitStrategy(); /* Tested (log OFF) best. */
        } else if ("YIELDING_WAIT".equals(env)) {
            waitStrategy = new YieldingWaitStrategy();  /* Tested (log OFF) best after SLEEPING_WAIT. */
        } else {
            waitStrategy = new BusySpinWaitStrategy();
        }
        return waitStrategy;
    }
}
