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

  public void report(long windowHead, long xor) {
    if (windowHead == this.toBeReported) {
      this.ack(windowHead, xor);
      this.toBeReported += this.window;
    } else {
      throw new IllegalArgumentException("Not monotonic reports. Expected: " + this.toBeReported + ", got: " + windowHead);
    }
  }

  public boolean ack(long ts, long xor) {
    final long lowerBound = this.startTs + this.window * ((ts - this.startTs) / this.window);
    final long updatedXor = xor ^ this.table.getOrDefault(lowerBound, 0L);
    if (updatedXor == 0) {
      this.table.remove(lowerBound);
      return true;
    } else {
      this.table.put(lowerBound, updatedXor);
      return false;
    }
  }

  public long min() {
    return this.table.isEmpty() ? this.toBeReported : Math.min(this.toBeReported, this.table.firstKey());
  }

  @Override
  public String toString() {
    return "TreeAckTable{" + "table=" + this.table +
            ", startTs=" + this.startTs +
            ", window=" + this.window +
            ", toBeReported=" + this.toBeReported +
            '}';
  }
}
