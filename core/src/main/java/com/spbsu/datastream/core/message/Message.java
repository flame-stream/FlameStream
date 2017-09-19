package com.spbsu.datastream.core.message;

public interface Message<T> {
  T payload();

  long tick();
}
