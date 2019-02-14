package com.spbsu.flamestream.runtime.utils;


import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class QueuedConsumer<T> implements Consumer<T> {
  private final Queue<T> result;
  private final AtomicInteger remainedSize;

  public QueuedConsumer(int expectedSize) {
    remainedSize = new AtomicInteger(expectedSize);
    result = new ConcurrentLinkedDeque<T>();
  }

  @Override
  public void accept(T value) {
    result.add(value);
  }

  public Stream<T> result() {
    synchronized (result) {
      return result.stream();
    }
  }

  public Queue<T> queue() {
    synchronized (result) {
      return result;
    }
  }
}

