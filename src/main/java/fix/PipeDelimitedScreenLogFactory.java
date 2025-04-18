package fix;

import org.slf4j.event.Level;
import quickfix.Log;
import quickfix.LogFactory;
import quickfix.SessionID;
import trading.common.AsyncLogger;

public class PipeDelimitedScreenLogFactory implements LogFactory {

    private final AsyncLogger logger;

    public PipeDelimitedScreenLogFactory(AsyncLogger logger) {
        this.logger = logger;
    }

    @Override
    public Log create(SessionID sessionID) {
        return new PipeDelimitedScreenLog(sessionID, logger);
    }

    static class PipeDelimitedScreenLog implements Log {
        private final AsyncLogger logger;
        private final String inLabel;
        private final String outLabel;
        private final String eventLabel;
        private final String errorLabel;

        public PipeDelimitedScreenLog(SessionID sessionID, AsyncLogger logger) {
            this.logger = logger;
            String[] split = sessionID.toString().split(":");
            String id = split[split.length -1];
            inLabel = id + " IN:";
            outLabel = id + " OUT:";
            eventLabel = id + " EVENT:";
            errorLabel = id + " ERROR:";
        }

        @Override
        public void onIncoming(String msg) {
            logger.logFIXMessage(System.currentTimeMillis(), inLabel, Level.INFO, msg);
        }

        @Override
        public void onOutgoing(String msg) {
            logger.logFIXMessage(System.currentTimeMillis(), outLabel, Level.INFO, msg);
        }

        @Override
        public void onEvent(String msg) {
            logger.logFIXMessage(System.currentTimeMillis(), eventLabel, Level.INFO, msg);
        }

        @Override
        public void onErrorEvent(String msg) {
            logger.logFIXMessage(System.currentTimeMillis(), errorLabel, Level.ERROR, msg);
        }

        @Override
        public void clear() {
            logger.logFIXMessage(System.currentTimeMillis(), errorLabel, Level.INFO,
                    "Log clear operation not supported.");
        }
    }

}