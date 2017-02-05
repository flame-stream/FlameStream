package com.spbsu.datastream.core.serialization;

/**
 * Created by marnikitta on 2/4/17.
 * copypasted from akka
 */
public interface Serializer {

  /**
   * Completely unique value to identify this implementation of Serializer, used to optimize network traffic.
   * Values from 0 to 16 are reserved for Akka internal usage.
   */
  int identifier();

  /**
   * Serializes the given object into an Array of Byte
   */
  byte[] toBinary(Object o);

  /**
   * Produces an object from an array of bytes, with an optional type-hint
   */
  Object fromBinary(byte[] bytes, Class<?> hint);
}
