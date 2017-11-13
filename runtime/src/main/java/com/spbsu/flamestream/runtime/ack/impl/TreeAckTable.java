package com.spbsu.flamestream.runtime.ack.impl;

import java.util.SortedMap;
import java.util.TreeMap;

final class TreeAckTable extends HeartbeatAckTable {
  // FIXME: 7/6/17 DO NOT BOX
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
