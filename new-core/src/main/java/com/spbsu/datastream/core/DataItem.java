package com.spbsu.datastream.core;

public interface DataItem<T> extends Traveller {
  Meta meta();

  T payload();
}
