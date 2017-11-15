package com.spbsu.flamestream.runtime.ack.api;

public final class CommitTick {
  private final long tickId;

  public CommitTick(long tickId) {
    this.tickId = tickId;
  }

  public long tickId() {
    return tickId;
  }

  @Override
  public String toString() {
    return "CommitTick{" + "tickId=" + tickId + '}';
  }
}
