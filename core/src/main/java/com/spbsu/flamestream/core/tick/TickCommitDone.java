package com.spbsu.flamestream.core.tick;

public final class TickCommitDone {
  private final long tickId;

  public TickCommitDone(long tickId) {
    this.tickId = tickId;
  }

  public long tickId() {
    return tickId;
  }

  @Override
  public String toString() {
    return "TickCommitDone{" +
            "tickId=" + tickId +
            '}';
  }
}
