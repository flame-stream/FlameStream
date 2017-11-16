package com.spbsu.flamestream.runtime.acker;

import java.util.SortedMap;
import java.util.TreeMap;

final class TreeAckTable extends HeartbeatAckTable {
  private final SortedMap<Long, Long> table;

  TreeAckTable(long startTs, long window) {
    super(startTs, window);
    this.table = new TreeMap<>();
  }

  @Override
  protected long tableMin() {
    return table.isEmpty() ? Long.MAX_VALUE : table.firstKey();
  }

  @Override
  public boolean ack(long timestamp, long xor) {
    final long lowerBound = startTs + window * ((timestamp - startTs) / window);
    final long updatedXor = xor ^ table.getOrDefault(lowerBound, 0L);
    if (updatedXor == 0) {
      table.remove(lowerBound);
      return true;
    } else {
      table.put(lowerBound, updatedXor);
      return false;
    }
  }
}
