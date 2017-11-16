package com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

/**
 * User: Artem
 * Date: 10.11.2017
 */
public class Accepted {
  private final GlobalTime globalTime;

  public Accepted(GlobalTime globalTime) {
    this.globalTime = globalTime;
  }

  public GlobalTime globalTime() {
    return this.globalTime;
  }
}
