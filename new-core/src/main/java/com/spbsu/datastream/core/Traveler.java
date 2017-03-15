package com.spbsu.datastream.core;

public interface Traveler {
  Meta meta();

  boolean isBroadcast();

  int hash();
}
