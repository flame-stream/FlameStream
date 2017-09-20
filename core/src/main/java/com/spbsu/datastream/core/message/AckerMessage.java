package com.spbsu.datastream.core.message;

public final class AckerMessage<T> implements Message<T> {
  private final T payload;
  private final long tick;

  public AckerMessage(T payload, long tick) {
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
    return "AckerMessage{" + "payload=" + payload +
            ", tick=" + tick +
            '}';
  }
}
