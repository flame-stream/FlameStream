package com.spbsu.flamestream.runtime.master.acker.api.commit;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class LastCommit {
  private final GlobalTime globalTime;

  public LastCommit(GlobalTime globalTime) {
    this.globalTime = globalTime;
  }

  public GlobalTime globalTime() {
    return globalTime;
  }

  @Override
  public String toString() {
    return "LastCommit{" +
            "globalTime=" + globalTime +
            '}';
  }
}
