package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class Heartbeat {
  private final GlobalTime time;
  private final String frontId;

  public Heartbeat(GlobalTime time, String frontId) {
    this.time = time;
    this.frontId = frontId;
  }

  public GlobalTime time() {
    return time;
  }

  public String frontId() {
    return frontId;
  }
}

