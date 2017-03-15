package com.spbsu.datastream.core;

public interface DataItem<T> extends Traveler {
  @Override
  Meta meta();

  T payload();

  @Override
  int hash();

  @Override
  default boolean isBroadcast() {
    return false;
  }
}
