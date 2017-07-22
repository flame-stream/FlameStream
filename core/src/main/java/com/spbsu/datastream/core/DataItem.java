package com.spbsu.datastream.core;

public interface DataItem<T> {
  Meta meta();

  T payload();

  long ack();
}
