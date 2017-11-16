package com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public final class Heartbeat {
  private final GlobalTime time;

  public Heartbeat(GlobalTime time) {
    this.time = time;
  }

  public GlobalTime time() {
    return time;
  }
}
