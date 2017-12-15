package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class Checkpoint {
  private final GlobalTime time;

  public Checkpoint(GlobalTime time) {
    this.time = time;
  }

  public GlobalTime time() {
    return time;
  }
}