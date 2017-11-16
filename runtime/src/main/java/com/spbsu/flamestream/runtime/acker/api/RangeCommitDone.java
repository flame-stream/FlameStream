package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.runtime.node.tick.range.HashRange;

public class RangeCommitDone {
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
