package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.GlobalTime;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public final class AckLedgerImpl implements AckLedger {
  private final TIntObjectMap<AckTable> tables = new TIntObjectHashMap<>();

  AckLedgerImpl(long startTs, long window, Iterable<Integer> fronts) {
    fronts.forEach(i -> this.tables.put(i, new AckTableImpl(startTs, window)));
  }

  @Override
  public void report(GlobalTime windowHead, long xor) {
    this.tables.get(windowHead.front()).report(windowHead.time(), xor);
  }

  @Override
  public GlobalTime min() {
    final int[] frontMin = {Integer.MAX_VALUE};
    final long[] timeMin = {Long.MAX_VALUE};
    this.tables.forEachEntry((f, table) -> {
      final long tmpMin = table.min();
      if (tmpMin < timeMin[0] || tmpMin == timeMin[0] && f < frontMin[0]) {
        frontMin[0] = f;
        timeMin[0] = tmpMin;
      }
      return true;
    });

    return new GlobalTime(timeMin[0], frontMin[0]);
  }

  @Override
  public void ack(GlobalTime windowHead, long xor) {
    this.tables.get(windowHead.front()).ack(windowHead.time(), xor);
  }

  @Override
  public String toString() {
    return "AckLedgerImpl{" + "tables=" + this.tables +
            '}';
  }
}
