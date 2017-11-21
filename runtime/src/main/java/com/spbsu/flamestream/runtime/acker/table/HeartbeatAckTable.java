package com.spbsu.flamestream.runtime.acker.table;

import com.spbsu.flamestream.runtime.acker.table.AckTable;

/**
 * User: Artem
 * Date: 05.11.2017
 */
public abstract class HeartbeatAckTable implements AckTable {
  protected final long startTs;
  protected final long window;

  private long minHeartbeatTs;

  HeartbeatAckTable(long startTs, long window) {
    this.startTs = startTs;
    this.window = window;
    this.minHeartbeatTs = startTs;
  }

  protected abstract long tableMin();

  @Override
  public void heartbeat(long heartbeatTs) {
    minHeartbeatTs = Math.max(minHeartbeatTs, heartbeatTs);
  }

  @Override
  public long min() {
    return Math.min(minHeartbeatTs, tableMin());
  }

  @Override
  public String toString() {
    return "AckTable{" + ", startTs=" + startTs + ", window=" + window + ", minHeartbeatTs=" + minHeartbeatTs + '}';
  }
}
