package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.GlobalTime;

public interface AckLedger {
  void report(GlobalTime windowHead, long xor);

  GlobalTime min();

  void ack(GlobalTime windowHead, long xor);
}
