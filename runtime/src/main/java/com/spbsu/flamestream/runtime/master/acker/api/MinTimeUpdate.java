package com.spbsu.flamestream.runtime.master.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.JobaTimes;

public class MinTimeUpdate {
  private final GlobalTime minTime;
  private final JobaTimes jobaTimes;

  public MinTimeUpdate(GlobalTime minTime, JobaTimes jobaTimes) {
    this.minTime = minTime;
    this.jobaTimes = jobaTimes;
  }

  public GlobalTime minTime() {
    return minTime;
  }

  @Override
  public String toString() {
    return "MinTimeUpdate{" + "minTime=" + minTime + '}';
  }

  public JobaTimes getJobaTimes() {
    return jobaTimes;
  }
}
