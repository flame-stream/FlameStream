package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.AckerMessage;
import com.spbsu.datastream.core.configuration.HashRange;

public final class CommitDone implements AckerMessage {
  private final HashRange participant;

  public CommitDone(HashRange participant) {
    this.participant = participant;
  }

  public HashRange committer() {
    return this.participant;
  }

  @Override
  public String toString() {
    return "CommitDone{" + "participant=" + this.participant +
            '}';
  }
}
