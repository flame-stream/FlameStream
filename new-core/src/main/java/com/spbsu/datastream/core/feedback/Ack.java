package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.GlobalTime;

public final class Ack {
  private final long xor;

  private final GlobalTime time;

  public Ack(final long xor, final GlobalTime time) {
    this.xor = xor;
    this.time = time;
  }

  public long xor() {
    return this.xor;
  }

  public GlobalTime time() {
    return this.time;
  }
}
