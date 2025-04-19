package trading.common;

import java.util.Queue;
import java.util.function.IntFunction;
import java.util.function.Supplier;

public class ObjectPool<T> {
    private final Supplier<T> objectFactory;

    private final Queue<T> pool;

    public ObjectPool(int size, Supplier<T> objectFactory) {
        this(size, objectFactory, SingleThreadRingBuffer::new);
//        this(size, objectFactory, OneToOneConcurrentArrayQueue::new);
    }

    public ObjectPool(int size, Supplier<T> objectFactory, IntFunction<Queue<T>> queueFactory) {
        this.pool = queueFactory.apply(size);
        for (int i = 0; i < size; i++) {
            pool.offer(objectFactory.get());
        }
        this.objectFactory = objectFactory;
    }

    public T acquire() {
        return pool.poll();
    }

    public void release(T obj) {
        pool.offer(obj);
    }

}
