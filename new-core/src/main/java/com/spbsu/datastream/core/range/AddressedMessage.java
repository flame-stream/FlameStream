package com.spbsu.datastream.core.range;

public final class AddressedMessage<T> {
  private final int hash;

  private final int tick;

  private final T payload;

  public AddressedMessage(final T payload,
                          final int hash,
                          final int tick) {
    this.payload = payload;
    this.hash = hash;
    this.tick = tick;
  }

  public int tick() {
    return this.tick;
  }

  public int hash() {
    return this.hash;
  }

  public T payload() {
    return this.payload;
  }
}
