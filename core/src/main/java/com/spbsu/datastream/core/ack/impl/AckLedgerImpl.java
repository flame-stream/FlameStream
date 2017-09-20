package com.spbsu.datastream.core.ack.impl;

import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.ack.AckLedger;
import com.spbsu.datastream.core.ack.AckTable;
import com.spbsu.datastream.core.tick.TickInfo;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public final class AckLedgerImpl implements AckLedger {
  private final TIntObjectMap<AckTable> tables = new TIntObjectHashMap<>();

  public AckLedgerImpl(TickInfo tickInfo) {
    tickInfo.graph()
            .frontBindings()
            .keySet()
            .forEach(i -> tables.put(i, new ArrayAckTable(tickInfo.startTs(), tickInfo.stopTs(), tickInfo.window())));
  }

  @Override
  public void report(GlobalTime windowHead, long xor) {
    tables.get(windowHead.front()).report(windowHead.time(), xor);
  }

  @Override
  public GlobalTime min() {
    final int[] frontMin = {Integer.MAX_VALUE};
    final long[] timeMin = {Long.MAX_VALUE};
    tables.forEachEntry((f, table) -> {
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
  public boolean ack(GlobalTime windowHead, long xor) {
    return tables.get(windowHead.front()).ack(windowHead.time(), xor);
  }

  @Override
  public String toString() {
    return "AckLedgerImpl{" + "tables=" + tables +
            '}';
  }
}
