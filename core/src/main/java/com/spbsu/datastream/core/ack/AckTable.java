package com.spbsu.datastream.core.ack;

/**
 * User: Artem
 * Date: 06.09.2017
 */
public interface AckTable {
  void report(long windowHead, long xor);

  void ack(long ts, long xor);

  long min();
}
