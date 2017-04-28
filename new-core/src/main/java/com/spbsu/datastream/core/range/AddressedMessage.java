package com.spbsu.datastream.core.range;

public final class AddressedMessage<T> {
  private final int hash;

  private final long tick;

  private final T payload;

  public AddressedMessage(final T payload,
                          final int hash,
                          final long tick) {
    this.payload = payload;
    this.hash = hash;
    this.tick = tick;
  }

  public long tick() {
    return this.tick;
  }

  public int hash() {
    return this.hash;
  }

  public T payload() {
    return this.payload;
  }
}
