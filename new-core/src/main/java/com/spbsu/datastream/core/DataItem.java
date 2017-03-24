package com.spbsu.datastream.core;

public interface DataItem<T> {
  long id();

  Meta meta();

  T payload();

  long rootId();
}
