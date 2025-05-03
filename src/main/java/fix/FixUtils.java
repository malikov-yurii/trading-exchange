package fix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.LogFactory;
import trading.common.AsyncLogger;
import trading.common.Utils;

public final class FixUtils {
    private static final Logger log = LoggerFactory.getLogger(FixUtils.class);

    private FixUtils() {
    }


    public static String getTestTag(long orderId) {
        return  "|11=" + orderId + "|";
    }

    public static LogFactory getFIXLoggerFactory(AsyncLogger asyncLogger) {
        LogFactory logFactory;
        String env = Utils.env("FIX_LOGGER", "PIPED");
        log.info("getFIXLoggerFactory. {}", env);
        if (env.equals("TAGGED")) {
            logFactory = new TaggedFIXLoggerFactory(asyncLogger);
        } else {
            logFactory = new PipeDelimitedFIXLoggerFactory(asyncLogger);
        }
        return logFactory;
    }
}
