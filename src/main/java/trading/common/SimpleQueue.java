package trading.common;

public interface SimpleQueue<T> {

    boolean offer(T item);

    T poll();

}
