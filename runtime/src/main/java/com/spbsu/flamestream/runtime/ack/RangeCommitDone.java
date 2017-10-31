package com.spbsu.flamestream.runtime.ack;

import com.spbsu.flamestream.runtime.range.HashRange;

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
    return "RangeCommitDone{" + "participant=" + participant + '}';
  }
}
