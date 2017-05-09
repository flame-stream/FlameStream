package com.spbsu.datastream.core.front;

public final class RawData<T> {
  private final T payload;

  public RawData(final T payload) {
    this.payload = payload;
  }

  public T payload() {
    return this.payload;
  }

  @Override
  public String toString() {
    return "RawData{" + "payload=" + this.payload +
            '}';
  }
}
