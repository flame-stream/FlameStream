package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

public class Ack {
  private final GlobalTime time;
  private final long xor;
  //private static final Tracing.Tracer tracer = Tracing.TRACING.forEvent("ack-create");

  public Ack(GlobalTime time, long xor) {
    this.time = time;
    this.xor = xor;
    //tracer.log(xor);
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
