package com.spbsu.flamestream.runtime.master.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

/**
 * Heartbeat means that there are no items with global time < heartbeat's time
 */
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

