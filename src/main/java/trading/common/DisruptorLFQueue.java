package trading.common;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DisruptorLFQueue<T> implements LFQueue<T> {

    private static final Logger log = LoggerFactory.getLogger(DisruptorLFQueue.class);
    private final String name;

    private final Disruptor<T> disruptor;
    private final RingBuffer<T> ringBuffer;
    private final BiConsumer<T, T> copyFromTo;

    public DisruptorLFQueue(Integer sizeInPowerOfTwo,
                            String name,
                            ProducerType producerType,
                            EventFactory<T> eventFactory,
                            BiConsumer<T, T> copyFromTo) {

        int power = Objects.requireNonNullElseGet(sizeInPowerOfTwo,
                () -> Integer.valueOf(Utils.env("DISRUPTOR_SIZE_POWER_OF_2", "10")));

        int ringBufferSize = 1 << power;
        disruptor = new Disruptor<>(
                eventFactory,
                ringBufferSize,
                Executors.defaultThreadFactory(),
                producerType,
                Utils.getDisruptorWaitStrategy()
        );
        ringBuffer = disruptor.getRingBuffer();
        this.name = name;
        this.copyFromTo = copyFromTo;
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
            T event = ringBuffer.get(sequence);
            copyFromTo.accept(request, event);
//            log.info(name + " | Offered sequence={} | {}", sequence, request);
        } catch (Exception ex) {
            log.error(name + " | Error offering request", ex);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    @Override
    public void subscribe(Consumer<T> consumer) {
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            try {
//                log.info(name + " | Passing to consumer sequence={} endOfBatch={} | {}", sequence, endOfBatch, event.request);
                consumer.accept(event);
            } catch (Exception ex) {
                log.error(name + " | Error handling event", ex);
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