package com.spbsu.flamestream.runtime.edge.akka;

public class RawData<T> {
  private final T data;

  public RawData(T data) {
    this.data = data;
  }

  public T data() {
    return data;
  }
}
