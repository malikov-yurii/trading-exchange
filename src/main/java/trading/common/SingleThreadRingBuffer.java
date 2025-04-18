package trading.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

public class SingleThreadRingBuffer<T> implements Queue<T> {
    private static final Logger log = LoggerFactory.getLogger(SingleThreadRingBuffer.class);
    private final Object[] buffer;
    private final int capacity;
    private int head = 0;
    private int tail = 0;
    private int size = 0;

    public SingleThreadRingBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = new Object[capacity];
    }

    @Override
    public boolean offer(T item) {
        if (size == capacity) {
            log.error(String.format("%s | SingleThreadRingBuffer is full. size: %s, capacity: %s, head: %s : %s, tail: %s : %s, 0: %s, 1:%s, 2:%s, 3:%s, 4:%s, 5:%s",
                    Thread.currentThread(), size, capacity, head, buffer[head], tail, buffer[tail], buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5]));
            System.exit(1);
//            throw new RuntimeException(String.format("%s | SingleThreadRingBuffer is full. size: %s, capacity: %s",
//                    Thread.currentThread(), size, capacity));
        }

        buffer[tail] = item;
        tail = (tail + 1) % capacity;
        size++;
        return true;
    }

    @Override
    public T remove() {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T poll() {
        if (size == 0) {
            log.error(String.format("%s | SingleThreadRingBuffer is empty. size: %s, capacity: %s, head: %s : %s, tail: %s : %s, 0: %s, 1:%s, 2:%s, 3:%s, 4:%s, 5:%s",
                    Thread.currentThread(), size, capacity, head, buffer[head], tail, buffer[tail], buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5]));
            System.exit(1);
            // TODO Investigate why it happens in perf test.. Should be only few orders at a time.
//            throw new RuntimeException(String.format("%s | SingleThreadRingBuffer is empty. size: %s, capacity: %s",
//                    Thread.currentThread(), size, capacity));
        }

        T item = (T) buffer[head];
        buffer[head] = null;
        head = (head + 1) % capacity;
        size--;
        return item;
    }

    @Override
    public T element() {
        return null;
    }

    @Override
    public T peek() {
        return null;
    }

    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return null;
    }



    @Override
    public boolean add(T t) {
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {

    }
}