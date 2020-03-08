package com.spbsu.flamestream.runtime.master.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.NodeTimes;

public class MinTimeUpdate {
  private final int trackingComponent;
  private final GlobalTime minTime;
  private final NodeTimes nodeTimes;

  public MinTimeUpdate(int trackingComponent, GlobalTime minTime, NodeTimes nodeTimes) {
    this.trackingComponent = trackingComponent;
    this.minTime = minTime;
    this.nodeTimes = nodeTimes;
  }

  public int trackingComponent() {
    return trackingComponent;
  }

  public GlobalTime minTime() {
    return minTime;
  }

  @Override
  public String toString() {
    return "MinTimeUpdate{" + "trackingComponent=" + trackingComponent + "minTime=" + minTime + '}';
  }

  public NodeTimes getNodeTimes() {
    return nodeTimes;
  }
}
