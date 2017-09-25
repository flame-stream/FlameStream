package com.spbsu.flamestream.core.ack;

import com.spbsu.flamestream.core.configuration.HashRange;

public final class RangeCommitDone {
  private final HashRange participant;

  public RangeCommitDone(HashRange participant) {
    this.participant = participant;
  }

  public HashRange committer() {
    return participant;
  }

  @Override
  public String toString() {
    return "RangeCommitDone{" + "participant=" + participant +
            '}';
  }
}
