package com.spbsu.flamestream.runtime.master.acker.api.commit;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class Prepare {
  private final GlobalTime globalTime;

  public Prepare(GlobalTime globalTime) {
    this.globalTime = globalTime;
  }

  public GlobalTime globalTime() {
    return globalTime;
  }

  @Override
  public String toString() {
    return "Prepare{" +
            "globalTime=" + globalTime +
            '}';
  }
}
