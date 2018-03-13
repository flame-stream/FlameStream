package com.spbsu.flamestream.runtime.graph.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class SinkPrepared {
  private final GlobalTime globalTime;

  public SinkPrepared(GlobalTime globalTime) {
    this.globalTime = globalTime;
  }

  public GlobalTime globalTime() {
    return this.globalTime;
  }
}
