package com.spbsu.flamestream.runtime.master.acker.api;

import java.util.List;
import java.util.stream.Stream;

public class CachedAcks implements AckerInputMessage {
  private List<Ack> acks;

  public CachedAcks(List<Ack> acks) {
    this.acks = acks;
  }

  public Stream<Ack> acks() {
    return acks.stream();
  }
}
