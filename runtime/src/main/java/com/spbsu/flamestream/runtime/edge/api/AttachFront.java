package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.runtime.FlameRuntime;

public class AttachFront<F extends Front> {
  private final String id;
  private final FlameRuntime.FrontInstance<F> instance;

  public AttachFront(String id, FlameRuntime.FrontInstance<F> instance) {
    this.id = id;
    this.instance = instance;
  }

  public String id() {
    return id;
  }

  public FlameRuntime.FrontInstance<F> instance() {
    return instance;
  }
}
