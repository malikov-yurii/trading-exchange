package fix;

import org.slf4j.event.Level;
import quickfix.Log;
import quickfix.LogFactory;
import quickfix.SessionID;
import trading.common.AsyncLogger;

public class PipeDelimitedFIXLoggerFactory implements LogFactory {

    private final AsyncLogger logger;

    public PipeDelimitedFIXLoggerFactory(AsyncLogger logger) {
        this.logger = logger;
    }

    @Override
    public Log create(SessionID sessionID) {
        return new PipeDelimitedFIXLogger(sessionID, logger);
    }

    static class PipeDelimitedFIXLogger implements Log {
        private final AsyncLogger logger;
        private final String inLabel;
        private final String outLabel;
        private final String eventLabel;
        private final String errorLabel;

        public PipeDelimitedFIXLogger(SessionID sessionID, AsyncLogger logger) {
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
            logger.logFIXMessage(inLabel, Level.INFO, msg);
        }

        @Override
        public void onOutgoing(String msg) {
            logger.logFIXMessage(outLabel, Level.INFO, msg);
        }

        @Override
        public void onEvent(String msg) {
            logger.logFIXMessage(eventLabel, Level.INFO, msg);
        }

        @Override
        public void onErrorEvent(String msg) {
            logger.logFIXMessage(errorLabel, Level.ERROR, msg);
        }

        @Override
        public void clear() {
            logger.logFIXMessage(errorLabel, Level.INFO, "Log clear operation not supported.");
        }
    }

}