package trading.common;

import org.slf4j.event.Level;

public interface AsyncLogger {

    void log(long time, String label, Level level, String msgTemplate, Object... args);

    void logFIXMessage(long time, String label, Level level, String msgTemplate);

}
