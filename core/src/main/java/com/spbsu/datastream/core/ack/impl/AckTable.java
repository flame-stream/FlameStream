package com.spbsu.datastream.core.ack.impl;

final class AckTable {
  private final long startTs;
  private final long window;
  private final long[] xorStorage;

  private int minPosition;
  private long toBeReported;

  AckTable(long startTs, long stopTs, long window) {
    this.startTs = startTs;
    this.window = window;

    final int xorStorageSize = Math.toIntExact((stopTs - startTs) / window);
    this.xorStorage = new long[xorStorageSize];
    this.minPosition = xorStorageSize;
    this.toBeReported = startTs;
  }

  void report(long windowHead, long xor) {
    if (windowHead == this.toBeReported) {
      this.ack(windowHead, xor);
      this.toBeReported += this.window;
    } else {
      throw new IllegalArgumentException("Not monotonic reports. Expected: " + this.toBeReported + ", got: " + windowHead);
    }
  }

  void ack(long ts, long xor) {
    final int position = Math.toIntExact(((ts - this.startTs) / this.window));
    final long updatedXor = xor ^ xorStorage[position];
    this.xorStorage[position] = updatedXor;

    if (updatedXor == 0 && xor != 0 && position == minPosition) {
      while (this.minPosition < this.xorStorage.length && this.xorStorage[this.minPosition] == 0)
        this.minPosition++;
    } else if (updatedXor != 0) {
      this.minPosition = Math.min(minPosition, position);
    }
  }

  long min() {
    return Math.min(this.toBeReported, this.startTs + this.window * this.minPosition);
  }

  @Override
  public String toString() {
    return "AckTableImpl{" +
            ", startTs=" + this.startTs +
            ", window=" + this.window +
            ", toBeReported=" + this.toBeReported +
            '}';
  }
}
