package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.FlameRuntime;

public class AttachRear<R extends Rear> {
  private final String id;
  private final FlameRuntime.RearInstance<R> rearInstance;

  public AttachRear(String id, FlameRuntime.RearInstance<R> rearInstance) {
    this.id = id;
    this.rearInstance = rearInstance;
  }

  public String id() {
    return id;
  }

  public FlameRuntime.RearInstance<R> instance() {
    return rearInstance;
  }
}
