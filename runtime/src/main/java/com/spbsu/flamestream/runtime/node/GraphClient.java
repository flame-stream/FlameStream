package com.spbsu.flamestream.runtime.node;

import com.spbsu.flamestream.core.Graph;

import java.util.Set;
import java.util.function.Consumer;

public interface GraphClient {
  Set<String> graphs(Consumer<Set<String>> watcher);

  Graph<?, ?> graphBy(String id);

  void put(String graphId, Graph<?, ?> graph);
}
