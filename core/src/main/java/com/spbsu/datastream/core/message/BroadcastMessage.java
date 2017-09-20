package com.spbsu.datastream.core.message;

public final class BroadcastMessage<T> implements Message<T> {
  private final T payload;
  private final long tick;

  public BroadcastMessage(T payload, long tick) {
    this.payload = payload;
    this.tick = tick;
  }

  @Override
  public T payload() {
    return payload;
  }

  @Override
  public long tick() {
    return tick;
  }

  @Override
  public String toString() {
    return "BroadcastMessage{" + "payload=" + payload +
            ", tick=" + tick +
            '}';
  }
}
