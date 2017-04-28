package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.GlobalTime;

import java.util.Set;

public interface AckLedger {
  void report(final GlobalTime windowHead);

  GlobalTime min();

  void ack(final GlobalTime windowHead, final long xor);

  Set<Integer> initHashes();

  long startTs();

  long window();
}
