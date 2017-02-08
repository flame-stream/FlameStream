package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class FlattenedGraph extends AbstractComposedGraph<AtomicGraph> {
  private FlattenedGraph(final Set<AtomicGraph> subGraphs,
                         final Map<OutPort, InPort> wires) {
    super(subGraphs, wires);
  }

  public static FlattenedGraph flattened(final Graph graph) {
    if (graph instanceof FlattenedGraph) {
      return (FlattenedGraph) graph;
    } else if (graph instanceof AtomicGraph) {
      return new FlattenedGraph(Collections.singleton((AtomicGraph) graph), Collections.emptyMap());
    } else if (graph instanceof ComposedGraph) {
      final ComposedGraphImpl composed = (ComposedGraphImpl) graph;

      final Set<FlattenedGraph> flatteneds = composed.subGraphs().stream()
              .map(FlattenedGraph::flattened).collect(Collectors.toSet());
      final Set<AtomicGraph> atomics = flatteneds.stream().map(FlattenedGraph::subGraphs)
              .flatMap(Set::stream).collect(Collectors.toSet());

      final Map<OutPort, InPort> downstreams = flatteneds.stream().map(FlattenedGraph::downstreams)
              .map(Map::entrySet).flatMap(Set::stream).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      downstreams.putAll(composed.downstreams());

      return new FlattenedGraph(atomics, downstreams);
    } else {
      throw new UnsupportedOperationException("Unsupported graph type");
    }
  }

  @Override
  public String toString() {
    return "FlattenedGraph{" + "upstreams=" + upstreams() +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
            '}';
  }
}
