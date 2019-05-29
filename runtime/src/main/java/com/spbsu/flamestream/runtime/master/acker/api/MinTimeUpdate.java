package com.spbsu.flamestream.runtime.master.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.NodeTimes;

public class MinTimeUpdate {
  private final GlobalTime minTime;
  private final NodeTimes nodeTimes;

  public MinTimeUpdate(GlobalTime minTime, NodeTimes nodeTimes) {
    this.minTime = minTime;
    this.nodeTimes = nodeTimes;
  }

  public GlobalTime minTime() {
    return minTime;
  }

  @Override
  public String toString() {
    return "MinTimeUpdate{" + "minTime=" + minTime + '}';
  }

  public NodeTimes getNodeTimes() {
    return nodeTimes;
  }
}
