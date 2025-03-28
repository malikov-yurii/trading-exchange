package trading.common;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class DisruptorLFQueue<T> implements LFQueue<T> {

    private static final Logger log = LoggerFactory.getLogger(DisruptorLFQueue.class);
    private final String name;

    private static class Event<T> {
        T request;
    }

    private static class EventFactoryImpl<T> implements EventFactory<Event<T>> {
        @Override
        public Event<T> newInstance() {
            return new Event<>();
        }
    }

    private final Disruptor<Event<T>> disruptor;
    private final RingBuffer<Event<T>> ringBuffer;

    /**
     * @param bufferSize   must be a power of 2
     * @param name
     * @param producerType
     */
    public DisruptorLFQueue(int bufferSize, String name, ProducerType producerType) {
        // Create the Disruptor with a single producer and a busy-spin wait strategy.
        disruptor = new Disruptor<>(
                new EventFactoryImpl<>(),
                bufferSize,
                Executors.defaultThreadFactory(),
                producerType,
                new BusySpinWaitStrategy()
        );
        ringBuffer = disruptor.getRingBuffer();
        this.name = name;
//        log.info("DisruptorLFQueue {} created with bufferSize {}", name, bufferSize);
    }

    @Override
    public void init() {
        disruptor.start();
//        log.info("init. Started [{}]", name);
    }

    @Override
    public void offer(T request) {
        long sequence = ringBuffer.next();
        try {
            Event<T> event = ringBuffer.get(sequence);
            event.request = request;
//            log.info(name + " Offered sequence={} | {}", sequence, request);
        } catch (Exception ex) {
            log.error(name + " Error offering request", ex);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    @Override
    public void subscribe(Consumer<T> consumer) {
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            try {
//                log.info(name + " Passing to consumer sequence={} endOfBatch={} | {}", sequence, endOfBatch, event.request);
                consumer.accept(event.request);
                event.request = null; // Clear to reduce GC pressure
            } catch (Exception ex) {
                log.error(name + " Error handling event", ex);
            }
        });
    }

    public void shutdown() {
        log.info("Terminating [{}]", name);
        try {
            disruptor.shutdown(2, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("Error shutting down disruptor. Halting now", e);
            disruptor.halt();
        }
        log.info("Terminated [{}]", name);
    }

}