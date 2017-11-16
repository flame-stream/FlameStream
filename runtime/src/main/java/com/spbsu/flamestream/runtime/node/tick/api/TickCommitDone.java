package com.spbsu.flamestream.runtime.node.tick.api;

public class TickCommitDone {
  private final long tickId;

  public TickCommitDone(long tickId) {
    this.tickId = tickId;
  }

  public long tickId() {
    return tickId;
  }

  @Override
  public String toString() {
    return "TickCommitDone{" + "tickId=" + tickId + '}';
  }
}
