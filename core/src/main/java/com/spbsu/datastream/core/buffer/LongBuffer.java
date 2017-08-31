package com.spbsu.datastream.core.buffer;

/**
 * User: Artem
 * Date: 31.08.2017
 */
public interface LongBuffer {
  void removeFirst();

  long get(int position);

  void put(int position, long value);

  int size();

  boolean isEmpty();
}
