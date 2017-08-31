package com.spbsu.datastream.core.buffer;

/**
 * User: Artem
 * Date: 31.08.2017
 */
public interface LongBuffer {
  void addLast(long value);

  void removeFirst();

  long get(int position);

  int size();

  boolean isEmpty();
}
