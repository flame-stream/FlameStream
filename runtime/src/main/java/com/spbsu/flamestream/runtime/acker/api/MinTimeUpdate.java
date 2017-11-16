package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class MinTimeUpdate {
  private final GlobalTime minTime;

  public MinTimeUpdate(GlobalTime minTime) {
    this.minTime = minTime;
  }

  public GlobalTime minTime() {
    return minTime;
  }

  @Override
  public String toString() {
    return "MinTimeUpdate{" + "minTime=" + minTime + '}';
  }
}
