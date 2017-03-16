package com.spbsu.datastream.example.invertedindex.models.long_containers;

/**
 * User: Artem
 * Date: 15.03.2017
 * Time: 19:27
 */
public interface LongContainer {
  long value();

  static long[] toLongArray(LongContainer[] longContainers) {
    final long[] result = new long[longContainers.length];
    for (int i = 0; i < longContainers.length; i++) {
      result[i] = longContainers[i].value();
    }
    return result;
  }
}
