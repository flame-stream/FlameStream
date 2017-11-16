package com.spbsu.flamestream.core.graph;

import java.util.Collections;
import java.util.Objects;

public abstract class AbstractAtomicGraph extends AbstractGraph implements AtomicGraph {
  private int localTime = 0;

  protected final int incrementLocalTimeAndGet() {
    this.localTime += 1;
    return localTime;
  }

  @Override
  public final ComposedGraph<AtomicGraph> flattened() {
    return new ComposedGraphImpl<>(Collections.singleton(this));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AbstractAtomicGraph that = (AbstractAtomicGraph) o;
    return that.inPorts().equals(inPorts())
            && that.outPorts().equals(outPorts());
  }

  @Override
  public int hashCode() {
    return Objects.hash(inPorts(), outPorts());
  }
}
