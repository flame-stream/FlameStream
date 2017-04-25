package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.GlobalTime;

import java.util.Collection;

public interface AckLedger {
  void report(final GlobalTime windowHead);

  GlobalTime min();

  void ack(final GlobalTime windowHead, final long xor);

  Collection<Integer> initHashes();

  long startTs();

  long window();
}
