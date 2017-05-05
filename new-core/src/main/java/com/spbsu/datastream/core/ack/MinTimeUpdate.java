package com.spbsu.datastream.core.ack;

import com.spbsu.datastream.core.GlobalTime;

public final class MinTimeUpdate {
  private final GlobalTime minTime;

  public MinTimeUpdate(final GlobalTime minTime) {
    this.minTime = minTime;
  }

  public GlobalTime minTime() {
    return this.minTime;
  }

  @Override
  public String toString() {
    return "MinTimeUpdate{" + "minTime=" + this.minTime +
            '}';
  }
}
