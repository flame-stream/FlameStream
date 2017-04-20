package com.spbsu.datastream.core.graph;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TheGraph implements ComposedGraph<AtomicGraph> {
  private final ComposedGraph<AtomicGraph> composedGraph;

  public TheGraph(final Graph graph) {
    if (!graph.isClosed()) {
      throw new IllegalArgumentException("Graph should be closed");
    }
    this.composedGraph = graph.flattened();
  }

  @Override
  public String toString() {
    return "TheGraph{" +
            ", downstreams=" + this.downstreams() +
            ", inPorts=" + this.inPorts() +
            ", outPorts=" + this.outPorts() +
            ", subGraphs=" + this.subGraphs() +
            '}';
  }

  @Override
  public Set<AtomicGraph> subGraphs() {
    return this.composedGraph.subGraphs();
  }

  @Override
  public Map<OutPort, InPort> downstreams() {
    return this.composedGraph.downstreams();
  }

  @Override
  public List<InPort> inPorts() {
    return this.composedGraph.inPorts();
  }

  @Override
  public List<OutPort> outPorts() {
    return this.composedGraph.outPorts();
  }

  @Override
  public ComposedGraph<AtomicGraph> flattened() {
    return this.composedGraph;
  }
}
