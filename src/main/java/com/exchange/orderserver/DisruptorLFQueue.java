package com.exchange.orderserver;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class DisruptorLFQueue<T> implements LFQueue<T> {

    private static final Logger log = LoggerFactory.getLogger(DisruptorLFQueue.class);
    private final String name;

    @Getter
    private static class Event<T> {
        private T request;
        public void set(T request) { this.request = request; }
    }

    private static class ClientRequestEventFactory<T> implements EventFactory<Event<T>> {
        @Override
        public Event<T> newInstance() {
            return new Event<>();
        }
    }

    private final Disruptor<Event<T>> disruptor;
    private final RingBuffer<Event<T>> ringBuffer;

    /**
     * @param bufferSize     must be a power of 2
     * @param name
     */
    public DisruptorLFQueue(int bufferSize, String name) {
        // Create the Disruptor with a single producer and a busy-spin wait strategy.
        disruptor = new Disruptor<>(
                new ClientRequestEventFactory<T>(),
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new BusySpinWaitStrategy()
        );
        // We do not attach a consumer handler since we poll on demand.
        ringBuffer = disruptor.getRingBuffer();
        this.name = name;
        log.info("DisruptorLFQueue {} created with bufferSize {}", name, bufferSize);
    }

    @Override
    public void init() {
        log.info("init. Starting [{}]", name);
        disruptor.start();
        log.info("init. Started [{}]", name);
    }

    @Override
    public void offer(T request) {
        long sequence = ringBuffer.next();
        try {
            Event<T> event = ringBuffer.get(sequence);
            event.set(request);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    @Override
    public void subscribe(Consumer<T> consumer) {
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            consumer.accept(event.getRequest());
            event.set(null); // Clear to reduce GC pressure
        });
    }

    /**
     * Shut down the disruptor.
     */
    public void shutdown() {
        disruptor.shutdown();
    }
}