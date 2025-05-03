package trading.common;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static String env(String envVar, String defaultValue) {
        return ObjectUtils.defaultIfNull(System.getenv(envVar), defaultValue);
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
