package com.spbsu.datastream.core.message;

import com.spbsu.datastream.core.message.Message;

public final class AckerMessage<T> implements Message<T> {
  private final T payload;
  private final long tick;

  public AckerMessage(T payload, long tick) {
    this.payload = payload;
    this.tick = tick;
  }

  @Override
  public T payload() {
    return this.payload;
  }

  @Override
  public long tick() {
    return this.tick;
  }

  @Override
  public String toString() {
    return "AckerMessage{" + "payload=" + this.payload +
            ", tick=" + this.tick +
            '}';
  }
}
