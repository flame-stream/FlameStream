package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.configuration.HashRange;

final class CommitDone {
  private final HashRange participant;

  public CommitDone(final HashRange participant) {
    this.participant = participant;
  }

  public HashRange committer() {
    return this.participant;
  }
}
