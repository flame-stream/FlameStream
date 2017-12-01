package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class Heartbeat {
  private final GlobalTime time;
  private final String frontId;
  private final String nodeId;

  public Heartbeat(GlobalTime time, String frontId, String nodeId) {
    this.time = time;
    this.frontId = frontId;
    this.nodeId = nodeId;
  }

  public String nodeId() {
    return nodeId;
  }

  public GlobalTime time() {
    return time;
  }

  public String frontId() {
    return frontId;
  }
}

