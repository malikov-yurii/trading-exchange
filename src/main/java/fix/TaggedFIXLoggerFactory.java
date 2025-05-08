package fix;

import lombok.Getter;
import org.slf4j.event.Level;
import quickfix.Log;
import quickfix.LogFactory;
import quickfix.SessionID;
import trading.common.AsyncLogger;

import java.util.concurrent.atomic.AtomicLong;

public class TaggedFIXLoggerFactory implements LogFactory {

    private final AsyncLogger logger;

    public TaggedFIXLoggerFactory(AsyncLogger logger) {
        this.logger = logger;
    }

    @Override
    public Log create(SessionID sessionID) {
        return new TaggedMsgLogger(sessionID, logger);
    }

    public static class TaggedMsgLogger implements Log {
        @Getter
        private static final AtomicLong outByteCounter = new AtomicLong();
        @Getter
        private static final AtomicLong inByteCounter = new AtomicLong();
        private final AsyncLogger logger;
        private final String inLabel;
        private final String outLabel;
        private final String eventLabel;
        private final String errorLabel;

        public TaggedMsgLogger(SessionID sessionID, AsyncLogger logger) {
            this.logger = logger;
            String[] split = sessionID.toString().split(":");
            String id = split[split.length -1];
            inLabel = id + " IN :";
            outLabel = id + " OUT:";
            eventLabel = id + " EVENT:";
            errorLabel = id + " ERROR:";
        }

        @Override
        public void onIncoming(String msg) {
            String label = inLabel;
            if (msg != null) {
                long offset = inByteCounter.addAndGet(msg.length());
                label += toLabel(offset);
            }
            logger.logTaggedFIXMessage(label, Level.INFO, msg);
        }

        @Override
        public void onOutgoing(String msg) {
            String label = outLabel;
            if (msg != null) {
                long offset = outByteCounter.addAndGet(msg.length());
                label += toLabel(offset);
            }
            logger.logTaggedFIXMessage(label, Level.INFO, msg);
        }

        @Override
        public void onEvent(String msg) {
            logger.logTaggedFIXMessage(eventLabel, Level.INFO, msg);
        }

        @Override
        public void onErrorEvent(String msg) {
            logger.logTaggedFIXMessage(errorLabel, Level.ERROR, msg);
        }

        @Override
        public void clear() {
            logger.logTaggedFIXMessage(errorLabel, Level.INFO, "Log clear operation not supported.");
        }
    }

    private static String toLabel(long offset) {
        return " " + String.format("%10s", offset) + " :";
    }

}