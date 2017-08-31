package com.spbsu.datastream.core.ack.impl;

import com.spbsu.datastream.core.buffer.LongBuffer;
import com.spbsu.datastream.core.buffer.impl.LongRingBuffer;

final class AckTable {
  private static final long MIN_TIME_NOT_INITIALIZED = Long.MAX_VALUE;

  private final LongBuffer buffer;
  private final long startTs;
  private final long window;

  private long minTs = MIN_TIME_NOT_INITIALIZED;
  private long positionTs;
  private long toBeReported;

  AckTable(long startTs, long stopTs, long window) {
    this.startTs = startTs;
    this.window = window;
    this.buffer = new LongRingBuffer(Math.toIntExact((stopTs - startTs) / window));

    this.toBeReported = startTs;
    this.positionTs = startTs;
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
    //code is not cleaned because it does not work for multiple workers
    final int position = Math.toIntExact(((ts - this.positionTs) / this.window));
    final long updatedXor = xor ^ (this.buffer.size() <= position ? 0L : this.buffer.get(position));
    if (updatedXor == 0 && !buffer.isEmpty() && xor != 0) {
      this.buffer.removeFirst();
      this.minTs = this.startTs + this.window * ((ts - this.startTs) / this.window) + this.window;
      this.positionTs += this.window;
    } else if (updatedXor != 0) {
      this.buffer.put(position, updatedXor);
      if (this.minTs == MIN_TIME_NOT_INITIALIZED) {
        this.minTs = this.startTs + this.window * ((ts - this.startTs) / this.window);
      } else {
        this.minTs = Math.min(minTs, this.startTs + this.window * ((ts - this.startTs) / this.window));
      }
    }
  }

  long min() {
    return this.buffer.isEmpty() ? this.toBeReported : Math.min(this.toBeReported, this.minTs);
  }

  @Override
  public String toString() {
    return "AckTableImpl{" + "table=" + this.buffer +
            ", startTs=" + this.startTs +
            ", window=" + this.window +
            ", toBeReported=" + this.toBeReported +
            '}';
  }
}
