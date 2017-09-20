package com.spbsu.datastream.core.graph;

import java.util.Collections;

public abstract class AbstractAtomicGraph implements AtomicGraph {
  private int localTime = 0;

  protected final int incrementLocalTimeAndGet() {
    this.localTime += 1;
    return localTime;
  }

  @Override
  public final ComposedGraph<AtomicGraph> flattened() {
    return new ComposedGraphImpl<>(Collections.singleton(this));
  }
}
