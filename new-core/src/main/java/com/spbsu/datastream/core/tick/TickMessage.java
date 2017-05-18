package com.spbsu.datastream.core.tick;

public final class TickMessage<T> {
  private final long tick;

  private final T payload;

  public TickMessage(long tick, T payload) {
    this.tick = tick;
    this.payload = payload;
  }

  public long tick() {
    return this.tick;
  }

  public T payload() {
    return this.payload;
  }

  @Override
  public String toString() {
    return "TickMessage{" + "tick=" + this.tick +
            ", payload=" + this.payload +
            '}';
  }
}
