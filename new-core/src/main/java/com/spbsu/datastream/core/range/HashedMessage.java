package com.spbsu.datastream.core.range;

public final class HashedMessage<T> {
  private final int hash;
  private final T payload;

  public HashedMessage(final int hash, final T payload) {
    this.hash = hash;
    this.payload = payload;
  }

  public int hash() {
    return this.hash;
  }

  public T payload() {
    return this.payload;
  }

  @Override
  public String toString() {
    return "HashedMessage{" + "hash=" + this.hash +
            ", payload=" + this.payload +
            '}';
  }
}
