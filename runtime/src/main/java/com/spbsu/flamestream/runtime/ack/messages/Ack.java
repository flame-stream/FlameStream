package com.spbsu.flamestream.runtime.ack.messages;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public final class Ack {
  private final GlobalTime time;
  private final long xor;
  private final boolean report;

  public Ack(GlobalTime time, long xor, boolean report) {
    this.time = time;
    this.xor = xor;
    this.report = report;
  }

  public GlobalTime time() {
    return time;
  }

  public long xor() {
    return xor;
  }

  public boolean isReport() {
    return report;
  }

  @Override
  public String toString() {
    return "Ack{" + "xor=" + xor + ", time=" + time + ", report=" + report + '}';
  }
}
