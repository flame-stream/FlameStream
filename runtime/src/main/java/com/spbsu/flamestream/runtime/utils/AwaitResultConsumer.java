package com.spbsu.flamestream.runtime.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class AwaitResultConsumer<T> implements Consumer<T> {
  private final List<T> result = new ArrayList<>();
  private final int expectedSize;

  public AwaitResultConsumer(int expectedSize) {
    this.expectedSize = expectedSize;
  }

  @Override
  public void accept(T value) {
    synchronized (result) {
      result.add(value);
      if (result.size() == expectedSize) {
        result.notifyAll();
      }
    }
  }

  public void await(long timeout, TimeUnit unit) throws InterruptedException {
    final long stop = System.currentTimeMillis() + unit.toMillis(timeout);
    synchronized (result) {
      while (result.size() < expectedSize && System.currentTimeMillis() < stop) {
        result.wait(unit.toMillis(timeout));
      }
    }
  }

  public Stream<T> result() {
    synchronized (result) {
      return result.stream();
    }
  }
}
