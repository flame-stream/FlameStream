package com.spbsu.flamestream.core.ack;

import com.spbsu.flamestream.core.configuration.HashRange;

public final class CommitDone {
  private final HashRange participant;

  public CommitDone(HashRange participant) {
    this.participant = participant;
  }

  public HashRange committer() {
    return participant;
  }

  @Override
  public String toString() {
    return "CommitDone{" + "participant=" + participant +
            '}';
  }
}