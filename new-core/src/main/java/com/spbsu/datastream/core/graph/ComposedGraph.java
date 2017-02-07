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

    this.downstreams = new HashMap<>(wires);
    this.upstreams = wires.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    this.inPorts = graphs.stream().map(Graph::inPorts)
            .flatMap(Set::stream).filter(wires::containsValue)
            .collect(Collectors.toSet());

    this.outPorts = graphs.stream().map(Graph::outPorts)
            .flatMap(Set::stream).filter(wires::containsKey)
            .collect(Collectors.toSet());
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final ComposedGraph that = (ComposedGraph) o;
    return Objects.equals(upstreams, that.upstreams) &&
            Objects.equals(downstreams, that.downstreams) &&
            Objects.equals(inPorts, that.inPorts) &&
            Objects.equals(outPorts, that.outPorts) &&
            Objects.equals(parts, that.parts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(upstreams, downstreams, inPorts, outPorts, parts);
  }

  @Override
  public String toString() {
    return "ComposedGraph{" + "upstreams=" + upstreams +
            ", downstreams=" + downstreams +
            ", inPorts=" + inPorts +
            ", outPorts=" + outPorts +
            ", parts=" + parts +
            '}';
  }
}
