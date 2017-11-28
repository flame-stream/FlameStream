package com.spbsu.flamestream.runtime.node.graph;

import com.spbsu.flamestream.core.Graph;

import java.util.Set;
import java.util.function.Consumer;

public interface GraphClient {
  Set<String> graphs(Consumer<Set<String>> watcher);

  Graph<?, ?> graphById(String id);

  void putGraph(String graphId, Graph<?, ?> graph);
}
