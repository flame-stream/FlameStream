package com.spbsu.datastream.core.graph;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TheGraph implements ComposedGraph<AtomicGraph> {
  private final ComposedGraph<AtomicGraph> composedGraph;

  public TheGraph(final FlatGraph flatGraph) {
    composedGraph = flatGraph;
  }

  @Override
  public String toString() {
    return "TheGraph{" +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
            '}';
  }

  @Override
  public Set<AtomicGraph> subGraphs() {
    return composedGraph.subGraphs();
  }

  @Override
  public Map<OutPort, InPort> downstreams() {
    return composedGraph.downstreams();
  }

  @Override
  public List<InPort> inPorts() {
    return composedGraph.inPorts();
  }

  @Override
  public List<OutPort> outPorts() {
    return composedGraph.outPorts();
  }
}
