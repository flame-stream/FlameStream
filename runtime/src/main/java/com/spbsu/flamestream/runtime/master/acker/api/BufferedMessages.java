package com.spbsu.flamestream.runtime.master.acker.api;

import java.util.List;
import java.util.stream.Stream;

public class BufferedMessages implements AckerInputMessage {
  private final List<Object> all;

  public BufferedMessages(List<Object> all) {
    this.all = all;
  }

  public Stream<Object> all() {
    return all.stream();
  }
}
