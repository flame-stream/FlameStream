package com.spbsu.flamestream.runtime.ack.impl;

import com.spbsu.flamestream.runtime.ack.AckTable;

/**
 * User: Artem
 * Date: 05.11.2017
 */
public abstract class WindowedAckTable implements AckTable {
  protected final long startTs;
  protected final long window;

  private long toBeReported;

  WindowedAckTable(long startTs, long window) {
    this.startTs = startTs;
    this.window = window;
    this.toBeReported = startTs;
  }

  protected abstract long tableMin();

  @Override
  public void report(long windowHead) {
    if (windowHead == toBeReported) {
      this.toBeReported += window;
    } else {
      throw new IllegalArgumentException("Not monotonic reports. Expected: " + toBeReported + ", got: " + windowHead);
    }
  }

  @Override
  public long min() {
    return Math.min(toBeReported, tableMin());
  }

  @Override
  public String toString() {
    return "AckTable{" + ", startTs=" + startTs + ", window=" + window + ", toBeReported=" + toBeReported + '}';
  }
}
