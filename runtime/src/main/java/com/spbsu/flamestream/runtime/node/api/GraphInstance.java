package com.spbsu.flamestream.runtime.node.api;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import org.jetbrains.annotations.NotNull;

public class GraphInstance implements Comparable<GraphInstance> {
  private final String id;
  private final ComposedGraph<AtomicGraph> graph;

  public GraphInstance(ComposedGraph<AtomicGraph> graph, String id) {
    this.id = id;
    this.graph = graph;
  }

  public String id() {
    return id;
  }

  public ComposedGraph<AtomicGraph> graph() {
    return graph;
  }

  @Override
  public int compareTo(@NotNull GraphInstance o) {
    return id.compareTo(o.id());
  }
}
