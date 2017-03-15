package com.spbsu.datastream.core;

public interface Control extends Traveler {

  @Override
  default boolean isBroadcast() {
    return true;
  }

  @Override
  default int hash() {
    return 0xFFFFFFFF;
  }
}
