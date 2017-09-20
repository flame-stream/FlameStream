package com.spbsu.datastream.core.ack.impl;

import com.spbsu.datastream.core.ack.AckTable;

import java.util.SortedMap;
import java.util.TreeMap;

final class TreeAckTable implements AckTable {
  // FIXME: 7/6/17 DO NOT BOX
  private final SortedMap<Long, Long> table;
  private final long startTs;
  private final long window;

  private long toBeReported;

  TreeAckTable(long startTs, long window) {
    this.startTs = startTs;
    this.window = window;
    this.table = new TreeMap<>();
    this.toBeReported = startTs;
  }

  @Override
  public void report(long windowHead, long xor) {
    if (windowHead == toBeReported) {
      ack(windowHead, xor);
      this.toBeReported += window;
    } else {
      throw new IllegalArgumentException("Not monotonic reports. Expected: " + toBeReported + ", got: " + windowHead);
    }
  }

  @Override
  public boolean ack(long ts, long xor) {
    final long lowerBound = startTs + window * ((ts - startTs) / window);
    final long updatedXor = xor ^ table.getOrDefault(lowerBound, 0L);
    if (updatedXor == 0) {
      table.remove(lowerBound);
      return true;
    } else {
      table.put(lowerBound, updatedXor);
      return false;
    }
  }

  @Override
  public long min() {
    return table.isEmpty() ? toBeReported : Math.min(toBeReported, table.firstKey());
  }

  @Override
  public String toString() {
    return "TreeAckTable{" + "table=" + table +
            ", startTs=" + startTs +
            ", window=" + window +
            ", toBeReported=" + toBeReported +
            '}';
  }
}
