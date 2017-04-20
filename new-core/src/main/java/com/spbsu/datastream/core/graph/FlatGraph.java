package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class FlatGraph implements ComposedGraph<AtomicGraph> {
  private final ComposedGraph<AtomicGraph> composedGraph;

  private FlatGraph(final Set<AtomicGraph> subGraphs,
                    final Map<OutPort, InPort> wires) {
    this.composedGraph = new ComposedGraphImpl<>(subGraphs, wires);
  }

  public static FlatGraph flattened(final Graph graph) {
    if (graph instanceof FlatGraph) {
      return (FlatGraph) graph;
    } else if (graph instanceof AtomicGraph) {
      return new FlatGraph(Collections.singleton((AtomicGraph) graph), Collections.emptyMap());
    } else if (graph instanceof ComposedGraph) {
      final ComposedGraph<? extends Graph> composed = (ComposedGraph<? extends Graph>) graph;

      final Set<FlatGraph> flatteneds = composed.subGraphs().stream()
              .map(FlatGraph::flattened).collect(Collectors.toSet());
      final Set<AtomicGraph> atomics = flatteneds.stream().map(FlatGraph::subGraphs)
              .flatMap(Set::stream).collect(Collectors.toSet());

      final Map<OutPort, InPort> downstreams = flatteneds.stream().map(FlatGraph::downstreams)
              .map(Map::entrySet).flatMap(Set::stream).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      downstreams.putAll(composed.downstreams());

      return new FlatGraph(atomics, downstreams);
    } else {
      throw new UnsupportedOperationException("Unsupported graph type");
    }
  }

  @Override
  public String toString() {
    return "FlatGraph{" +
            ", downstreams=" + this.downstreams() +
            ", inPorts=" + this.inPorts() +
            ", outPorts=" + this.outPorts() +
            ", subGraphs=" + this.subGraphs() +
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
