package com.spbsu.flamestream.core.graph;

import java.util.Map;
import java.util.Set;

public interface ComposedGraph<T extends Graph> extends Graph {
  Set<T> subGraphs();

  Map<OutPort, InPort> downstreams();
}
