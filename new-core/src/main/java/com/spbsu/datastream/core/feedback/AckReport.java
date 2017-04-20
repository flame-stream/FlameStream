package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.GlobalTime;

public final class AckReport {
  private final GlobalTime from;

  private final GlobalTime to;

  private final long xor;

  public AckReport(final GlobalTime from, final GlobalTime to, final long xor) {
    this.from = from;
    this.to = to;
    this.xor = xor;
  }

  public GlobalTime from() {
    return this.from;
  }

  public GlobalTime to() {
    return this.to;
  }

  public long xor() {
    return this.xor;
  }

  @Override
  public String toString() {
    return "AckReport{" + "from=" + this.from +
            ", to=" + this.to +
            ", xor=" + this.xor +
            '}';
  }
}

