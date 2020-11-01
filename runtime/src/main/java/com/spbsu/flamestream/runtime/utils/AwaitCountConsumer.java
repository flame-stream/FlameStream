package com.spbsu.flamestream.runtime.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 03.01.2018
 */
public class AwaitCountConsumer implements Consumer<Object> {
  private final AtomicInteger counter = new AtomicInteger();
  private final int expectedSize;
  private long acceptedAt = Long.MIN_VALUE;

  public AwaitCountConsumer(int expectedSize) {
    this.expectedSize = expectedSize;
  }

  @Override
  public void accept(Object o) {
    acceptedAt = System.currentTimeMillis();
    if (counter.incrementAndGet() == expectedSize) {
      synchronized (counter) {
        counter.notifyAll();
      }
    }
  }

  public int got() {
    return counter.get();
  }

  public int expected() {
    return expectedSize;
  }

  public void await(long timeout, TimeUnit unit) throws InterruptedException {
    synchronized (counter) {
      acceptedAt = System.currentTimeMillis();
      long millisToWait = unit.toMillis(timeout);
      while (counter.get() < expectedSize && 0 < millisToWait) {
        counter.wait(unit.toMillis(millisToWait));
        millisToWait = acceptedAt + unit.toMillis(timeout) - System.currentTimeMillis();
      }
    }
  }
}
