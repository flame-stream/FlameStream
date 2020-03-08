package com.spbsu.flamestream.runtime.master.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

public class Ack implements AckerInputMessage {
  private static final Tracing.Tracer tracer = Tracing.TRACING.forEvent("ack-create");
  private final int trackingComponent;
  private final GlobalTime time;
  private final long xor;

  public Ack(int trackingComponent, GlobalTime time, long xor) {
    this.trackingComponent = trackingComponent;
    this.time = time;
    this.xor = xor;
    tracer.log(xor);
  }

  public int trackingComponent() {
    return trackingComponent;
  }

  public GlobalTime time() {
    return time;
  }

  public long xor() {
    return xor;
  }

  @Override
  public String toString() {
    return "Ack{" + ", trackingComponent=" + trackingComponent + ", xor=" + xor + ", time=" + time + '}';
  }
}
