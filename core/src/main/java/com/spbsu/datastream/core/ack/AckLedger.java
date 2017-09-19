package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.meta.GlobalTime;

public interface AckLedger {
  void report(GlobalTime windowHead, long xor);

  GlobalTime min();

  boolean ack(GlobalTime windowHead, long xor);
}
