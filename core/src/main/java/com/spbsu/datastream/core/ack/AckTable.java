package com.spbsu.datastream.core.ack;

/**
 * User: Artem
 * Date: 06.09.2017
 */
public interface AckTable {
  void report(long windowHead, long xor);

  /**
   * @return true if min time may be updated
   */
  boolean ack(long ts, long xor);

  long min();
}
