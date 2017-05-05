package com.spbsu.datastream.core.range;

public final class AddressedMessage<T> {
  private final int hash;

  private final boolean broadcast;

  private final long tick;

  private final T payload;

  public AddressedMessage(final T payload,
                          final int hash,
                          final long tick) {
    this.payload = payload;
    this.hash = hash;
    this.tick = tick;
    this.broadcast = false;
  }

  public AddressedMessage(final long tick, final T payload) {
    this.hash = 0;
    this.tick = tick;
    this.payload = payload;
    this.broadcast = true;
  }

  public boolean isBroadcast() {
    return this.broadcast;
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
