package com.spbsu.flamestream.runtime.node.front.api;

import com.spbsu.flamestream.runtime.FlameRuntime;

public class FrontInstance {
  private final String frontId;
  private final FlameRuntime.Front front;

  public FrontInstance(String frontId, FlameRuntime.Front front) {
    this.frontId = frontId;
    this.front = front;
  }

  public String frontId() {
    return frontId;
  }

  public FlameRuntime.Front front() {
    return front;
  }
}
