package trading.common;

import org.slf4j.event.Level;

public interface AsyncLogger {

    void log(String label, Level level, String msgTemplate, Object... args);

    void logFIXMessage(String label, Level level, String msgTemplate);

    void logTaggedFIXMessage(String label, Level level, String msgTemplate);

}
