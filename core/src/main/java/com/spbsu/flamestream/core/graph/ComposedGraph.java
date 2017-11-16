package com.spbsu.flamestream.core.graph.composed;

import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.Map;
import java.util.Set;

public interface ComposedGraph<T extends Graph> extends Graph {
  Set<T> subGraphs();

  Map<OutPort, InPort> downstreams();
}
