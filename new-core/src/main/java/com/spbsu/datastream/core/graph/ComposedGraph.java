package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by marnikitta on 2/6/17.
 */
public class ComposedGraph implements Graph {
  private final Map<InPort, OutPort> upstreams;
  private final Map<OutPort, InPort> downstreams;

  private final Set<InPort> inPorts;
  private final Set<OutPort> outPorts;

  private final Set<Graph> parts;

  public ComposedGraph(final Set<Graph> graph) {
    this(graph, Collections.emptyMap());
  }

  public ComposedGraph(final Graph graph,
                       final OutPort from,
                       final InPort to) {
    this(Collections.singleton(graph), Collections.singletonMap(from, to));
  }

  public ComposedGraph(final Set<Graph> graphs,
                       final Map<OutPort, InPort> wires) {
    assertCorrectWires(graphs, wires);

    this.parts = new HashSet<>(graphs);

    final Map<InPort, OutPort> mergedUpstreams = graphs.stream().map(Graph::upstreams)
            .map(Map::entrySet).flatMap(Collection::stream)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    wires.forEach((from, to) -> mergedUpstreams.put(to, from));
    this.upstreams = mergedUpstreams;

    final Map<OutPort, InPort> mergedDownstreams = graphs.stream().map(Graph::downstreams)
            .map(Map::entrySet).flatMap(Collection::stream)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    wires.forEach(mergedDownstreams::put);
    this.downstreams = mergedDownstreams;

    final Set<InPort> cleanedInPorts = graphs.stream().map(Graph::inPorts)
            .flatMap(Set::stream).collect(Collectors.toSet());
    cleanedInPorts.removeAll(wires.values());
    this.inPorts = cleanedInPorts;

    final Set<OutPort> cleanedOutPorts = graphs.stream().map(Graph::outPorts)
            .flatMap(Set::stream).collect(Collectors.toSet());
    cleanedOutPorts.removeAll(wires.keySet());
    this.outPorts = cleanedOutPorts;
  }

  private static void assertCorrectWires(final Set<Graph> graphs,
                                         final Map<OutPort, InPort> wires) {
    wires.forEach((from, to) -> assertCorrectWire(graphs, from, to));
  }

  private static void assertCorrectWire(final Set<Graph> graphs, final OutPort from, final InPort to) {
    if (!graphs.stream().anyMatch(graph -> graph.outPorts().contains(from))) {
      throw new WiringException();
    }

    if (!graphs.stream().anyMatch(graph -> graph.inPorts().contains(to))) {
      throw new WiringException();
    }
  }

  public Set<Graph> parts() {
    return Collections.unmodifiableSet(parts);
  }

  @Override
  public Set<InPort> inPorts() {
    return this.inPorts;
  }

  @Override
  public Set<OutPort> outPorts() {
    return this.outPorts;
  }

  @Override
  public Map<OutPort, InPort> downstreams() {
    return this.downstreams;
  }

  @Override
  public Map<InPort, OutPort> upstreams() {
    return this.upstreams;
  }
}
