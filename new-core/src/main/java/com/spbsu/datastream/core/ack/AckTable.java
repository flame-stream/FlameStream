package com.spbsu.datastream.core.ack;

public interface AckTable {
  long min();

  long window();

  long start();

  void report(long windowHead, long xor);

  void ack(final long windowHead, final long xor);
}
