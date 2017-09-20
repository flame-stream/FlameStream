package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.meta.GlobalTime;

public final class Ack {
  private final long xor;

  private final GlobalTime time;

  public Ack(long xor, GlobalTime time) {
    this.xor = xor;
    this.time = time;
  }

  public long xor() {
    return xor;
  }

  public GlobalTime time() {
    return time;
  }

  @Override
  public String toString() {
    return "Ack{" + "xor=" + xor +
            ", time=" + time +
            '}';
  }
}
