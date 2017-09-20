package com.spbsu.datastream.core.ack.impl;

import com.spbsu.datastream.core.ack.AckTable;

final class ArrayAckTable implements AckTable {
  private final long startTs;
  private final long window;
  private final long[] xorStorage;

  private int minPosition;
  private long toBeReported;

  ArrayAckTable(long startTs, long stopTs, long window) {
    this.startTs = startTs;
    this.window = window;

    final int xorStorageSize = Math.toIntExact((stopTs - startTs) / window);
    this.xorStorage = new long[xorStorageSize];
    this.minPosition = xorStorageSize;
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
    final int position = Math.toIntExact(((ts - startTs) / window));
    final long updatedXor = xor ^ xorStorage[position];
    xorStorage[position] = updatedXor;

    if (updatedXor == 0 && xor != 0 && position == minPosition) {
      while (minPosition < xorStorage.length && xorStorage[minPosition] == 0)
        this.minPosition++;
      return true;
    } else if (updatedXor != 0 && position < minPosition) {
      this.minPosition = position;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public long min() {
    return Math.min(toBeReported, startTs + window * minPosition);
  }

  @Override
  public String toString() {
    return "ArrayAckTable{" +
            ", startTs=" + startTs +
            ", window=" + window +
            ", toBeReported=" + toBeReported +
            '}';
  }
}
