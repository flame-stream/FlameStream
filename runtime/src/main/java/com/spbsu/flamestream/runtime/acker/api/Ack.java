package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class Ack {
  private final GlobalTime time;
  private final long xor;

  public Ack(GlobalTime time, long xor) {
    this.time = time;
    this.xor = xor;
  }

  public GlobalTime time() {
    return time;
  }

  public long xor() {
    return xor;
  }

  @Override
  public String toString() {
    return "Ack{" + "xor=" + xor + ", time=" + time + '}';
  }
}
