package com.spbsu.flamestream.runtime.node.api;

import com.spbsu.flamestream.core.graph.Graph;

public class NewGraph {
  private final Graph graph;
  private final String id;

  public NewGraph(Graph graph, String id) {
    this.graph = graph;
    this.id = id;
  }

  public String id() {
    return id;
  }

  public Graph graph() {
    return graph;
  }
}
