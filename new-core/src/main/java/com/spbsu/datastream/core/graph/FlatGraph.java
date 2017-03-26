package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

public final class FlatGraph extends AbstractComposedGraph<AtomicGraph> {

  private FlatGraph(final Set<AtomicGraph> subGraphs,
                    final Map<OutPort, InPort> wires) {
    super(subGraphs, wires);
  }

  //for deep copy only
  private FlatGraph(final Map<InPort, OutPort> upstreams,
                    final Map<OutPort, InPort> downstreams,
                    final List<InPort> inPorts,
                    final List<OutPort> outPorts,
                    final Set<AtomicGraph> subGraphs) {
    super(upstreams, downstreams, inPorts, outPorts, subGraphs);
  }

  public static FlatGraph flattened(final Graph graph) {
    if (graph instanceof FlatGraph) {
      return (FlatGraph) graph;
    } else if (graph instanceof AtomicGraph) {
      return new FlatGraph(Collections.singleton((AtomicGraph) graph), Collections.emptyMap());
    } else if (graph instanceof ComposedGraph) {
      final ComposedGraphImpl composed = (ComposedGraphImpl) graph;

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
    return "FlatGraph{" + "upstreams=" + upstreams() +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
            '}';
  }
}
