package com.spbsu.flamestream.core.graph;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractGraph implements Graph {
  @Override
  public final Graph fuse(Graph that, OutPort from, InPort to) {
    return compose(that).wire(from, to);
  }

  @Override
  public final Graph compose(Graph that) {
    final Set<Graph> graphs = new HashSet<>();
    graphs.add(this);
    graphs.add(that);
    return new ComposedGraphImpl<>(graphs);
  }

  @Override
  public final Graph wire(OutPort from, InPort to) {
    return new ComposedGraphImpl<>(this, from, to);
  }
}
