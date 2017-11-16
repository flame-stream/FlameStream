package com.spbsu.flamestream.runtime.node.tick.range.api;

import com.spbsu.flamestream.core.graph.AtomicGraph;

public class AtomicCommitDone {
  private final AtomicGraph atomicGraph;

  public AtomicCommitDone(AtomicGraph atomicGraph) {
    this.atomicGraph = atomicGraph;
  }

  public AtomicGraph graph() {
    return atomicGraph;
  }

  @Override
  public String toString() {
    return "AtomicCommitDone{" + "atomicGraph=" + atomicGraph + '}';
  }
}
