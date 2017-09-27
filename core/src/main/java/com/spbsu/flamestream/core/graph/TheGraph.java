package com.spbsu.flamestream.core.graph;

import java.util.*;

public final class TheGraph {
  private final ComposedGraph<AtomicGraph> composedGraph;

  private final Map<Integer, InPort> frontBindings;

  public TheGraph(Graph graph, Map<Integer, InPort> frontBindings) {
    TheGraph.assertFrontMapping(graph, frontBindings);

    this.composedGraph = graph.flattened();
    this.frontBindings = new HashMap<>(frontBindings);
  }

  private static void assertFrontMapping(Graph tail,
                                         Map<Integer, InPort> frontDownstreams) {
    final Set<InPort> bindPorts = new HashSet<>(frontDownstreams.values());
    final Set<InPort> freePorts = new HashSet<>(tail.inPorts());

    if (!freePorts.stream().allMatch(bindPorts::contains)) {
      throw new IllegalArgumentException("Not all inPorts are binded");
    }

    if (!bindPorts.stream().allMatch(freePorts::contains)) {
      throw new IllegalArgumentException("Unknow port binding");
    }
  }

  public ComposedGraph<AtomicGraph> graph() {
    return composedGraph;
  }

  public Map<Integer, InPort> frontBindings() {
    return Collections.unmodifiableMap(frontBindings);
  }
}
