package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class Heartbeat {
  private final GlobalTime time;

  public Heartbeat(GlobalTime time) {
    this.time = time;
  }

  public GlobalTime time() {
    return time;
  }

  @Override
  public String toString() {
    return "Heartbeat{" +
            "time=" + time +
            '}';
  }
}

