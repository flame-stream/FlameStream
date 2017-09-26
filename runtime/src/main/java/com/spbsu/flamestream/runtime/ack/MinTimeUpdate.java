package com.spbsu.flamestream.runtime.ack;

import com.spbsu.flamestream.core.meta.GlobalTime;

public final class MinTimeUpdate {
  private final GlobalTime minTime;

  public MinTimeUpdate(GlobalTime minTime) {
    this.minTime = minTime;
  }

  public GlobalTime minTime() {
    return minTime;
  }

  @Override
  public String toString() {
    return "MinTimeUpdate{" + "minTime=" + minTime +
            '}';
  }
}
