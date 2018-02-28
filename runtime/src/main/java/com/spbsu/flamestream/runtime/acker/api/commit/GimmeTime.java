package com.spbsu.flamestream.runtime.acker.api.commit;

public class GimmeTime {
  private final long awaitCommittedCount;

  public GimmeTime(long awaitCommittedCount) {
    this.awaitCommittedCount = awaitCommittedCount;
  }

  public long awaitCommittedCount() {
    return this.awaitCommittedCount;
  }
}
