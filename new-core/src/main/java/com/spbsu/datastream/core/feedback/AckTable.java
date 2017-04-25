package com.spbsu.datastream.core.feedback;

public interface AckTable {
  void report(final long windowHead);

  long min();

  long window();

  long startTs();

  void ack(final long windowHead, final long xor);
}
