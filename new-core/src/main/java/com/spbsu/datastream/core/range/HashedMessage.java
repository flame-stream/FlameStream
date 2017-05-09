package com.spbsu.datastream.core.range;

public final class HashedMessage<T> {
  private final int hash;
  private final T payload;
  private final boolean broadcast;

  public HashedMessage(final int hash, final T payload) {
    this.hash = hash;
    this.payload = payload;
    this.broadcast = false;
  }

  public HashedMessage(final T payload) {
    this.hash = -1;
    this.payload = payload;
    this.broadcast = true;
  }

  public int hash() {
    return this.hash;
  }

  public T payload() {
    return this.payload;
  }

  public boolean isBroadcast() {
    return this.broadcast;
  }

  @Override
  public String toString() {
    return "HashedMessage{" + "hash=" + this.hash +
            ", payload=" + this.payload +
            ", broadcast=" + this.broadcast +
            '}';
  }
}
