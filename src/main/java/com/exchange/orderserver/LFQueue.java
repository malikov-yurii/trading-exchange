package com.exchange.orderserver;

import java.util.function.Consumer;

public interface LFQueue<T> {

    void offer(T t);

    default T poll() { return null; }

    default void init() {}

    default void subscribe(Consumer<T> consumer) { }

}
