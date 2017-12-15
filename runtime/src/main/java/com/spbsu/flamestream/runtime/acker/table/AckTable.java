package com.spbsu.flamestream.runtime.acker.table;

/**
 * User: Artem
 * Date: 06.09.2017
 */
public interface AckTable {
  /**
   * @param timestamp of the ack
   * @param xor       number to be XORed
   * @return true if min time may be updated
   */
  boolean ack(long timestamp, long xor);

  /**
   * @return current min timestamp within this table
   */
  long tryPromote(long upTo);
}
