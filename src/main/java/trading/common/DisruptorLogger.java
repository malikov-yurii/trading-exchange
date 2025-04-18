package trading.common;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class DisruptorLogger implements AsyncLogger {

    private static final Logger log = LoggerFactory.getLogger(DisruptorLogger.class);
    private static final ZoneId SYSTEM_ZONE = ZoneId.systemDefault();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final Disruptor<LogRecord> disruptor;
    private final RingBuffer<LogRecord> ringBuffer;
    private final Function<LogRecord, String> toFormattedString;
    private final Function<LogRecord, String> toFIXMessageString;

    public DisruptorLogger(int sizeInPowerOfTwo) {

        disruptor = new Disruptor<>(
                LogRecord::new,
                1<< sizeInPowerOfTwo,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                Utils.getDisruptorWaitStrategy()
        );

        ringBuffer = disruptor.getRingBuffer();

        this.toFormattedString = event -> toLogLine(event, String.format(event.msg, event.args));

        this.toFIXMessageString = event -> toLogLine(event, event.msg.replace('\u0001', '|'));
    }

    private static String toLogLine(LogRecord event, String msg) {
        StringBuilder sb = new StringBuilder(128);

        LocalTime time = Instant.ofEpochMilli(event.date).atZone(SYSTEM_ZONE).toLocalTime();
        TIME_FORMATTER.formatTo(time, sb);

        sb.append(' ');

        if (event.level != null) {
            sb.append(event.level).append(' ');
        }

        if (event.label != null) {
            sb.append(event.label).append(' ');
        }

        sb.append(msg);
        return sb.toString();
    }

    public void init() {
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            try {
                String logLine = event.formatter.apply(event);

                if (event.level == Level.ERROR) {
                    System.err.println(logLine);
                } else {
                    System.out.println(logLine);
                }
            } catch (Exception ex) {
                log.error("Error handling event", ex);
            }
        });
        disruptor.start();
//        log.info("init. Started [{}]", name);
    }

    @Override
    public void log(long time, String label, Level level, String msgTemplate, Object... args) {
        if (log.isEnabledForLevel(level)) {
            logLine(time, label, level, msgTemplate, args, toFormattedString);
        }
    }

    @Override
    public void logFIXMessage(long time, String label, Level level, String msgTemplate) {
        if (log.isEnabledForLevel(level)) {
            logLine(time, label, null, msgTemplate, null, toFIXMessageString);
        }
    }

    private void logLine(long time, String label, Level level, String msgTemplate, Object[] args, Function<LogRecord, String> formatter) {
        long sequence = ringBuffer.next();
        try {
            LogRecord logRecord = ringBuffer.get(sequence);
            logRecord.label = label;
            logRecord.msg = msgTemplate;
            logRecord.args = args;
            logRecord.date = time;
            logRecord.level = level;
            logRecord.formatter = formatter;
        } catch (Exception ex) {
            log.error("Error offering request", ex);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    public void shutdown() {
        log.info("Terminating");
        try {
            disruptor.shutdown(2, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("Error shutting down disruptor. Halting now", e);
            disruptor.halt();
        }
        log.info("Terminated");
    }

    private static class LogRecord {
        String label;
        String msg;
        Object[] args;
        long date;
        Level level;
        Function<LogRecord, String> formatter;
    }

}