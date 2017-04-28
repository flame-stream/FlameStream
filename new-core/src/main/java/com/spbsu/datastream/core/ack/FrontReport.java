package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.GlobalTime;

public final class FrontReport {

  private final GlobalTime globalTime;

  private final long xor;

  /***
   * @param globalTime start time of the report.
   * @param xor xor of the dataItem acks, generated in the window [globalTime, globalTime + dt]
   */
  public FrontReport(final GlobalTime globalTime, final long xor) {
    this.globalTime = globalTime;
    this.xor = xor;
  }

  public GlobalTime globalTime() {
    return this.globalTime;
  }

  public long xor() {
    return this.xor;
  }

  @Override
  public String toString() {
    return "FrontReport{" + "globalTime=" + this.globalTime +
            ", xor=" + this.xor +
            '}';
  }
}

