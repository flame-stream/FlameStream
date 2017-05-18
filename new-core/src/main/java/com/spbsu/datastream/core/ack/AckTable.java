package com.spbsu.datastream.core.ack;

interface AckTable {
  long min();

  long window();

  long start();

  void report(long windowHead, long xor);

  void ack(long windowHead, long xor);
}
