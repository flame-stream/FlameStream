package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

final class ComposedGraphImpl<T extends Graph> implements ComposedGraph<T> {
  private final Map<OutPort, InPort> downstreams;

  private final List<InPort> inPorts;
  private final List<OutPort> outPorts;

  private final Set<T> subGraphs;


  ComposedGraphImpl(final ComposedGraphImpl<T> that) {
    this(that.downstreams, that.inPorts, that.outPorts, that.subGraphs);
  }

  ComposedGraphImpl(final Set<T> graphs) {
    this(graphs, Collections.emptyMap());
  }

  ComposedGraphImpl(final T graph,
                    final OutPort from,
                    final InPort to) {
    this(Collections.singleton(graph), Collections.singletonMap(from, to));
  }

  ComposedGraphImpl(final Set<T> graphs,
                    final Map<OutPort, InPort> wires) {
    assertCorrectWires(graphs, wires);

    this.subGraphs = new HashSet<>(graphs);

    this.downstreams = new HashMap<>(wires);

    this.inPorts = graphs.stream().map(Graph::inPorts)
            .flatMap(Collection::stream).filter(port -> !downstreams.containsValue(port))
            .collect(Collectors.toList());

    this.outPorts = graphs.stream().map(Graph::outPorts)
            .flatMap(List::stream).filter(port -> !downstreams.containsKey(port))
            .collect(Collectors.toList());
  }

  ComposedGraphImpl(final Map<OutPort, InPort> downstreams,
                    final List<InPort> inPorts,
                    final List<OutPort> outPorts,
                    final Set<T> subGraphs) {
    this.downstreams = downstreams;
    this.inPorts = inPorts;
    this.outPorts = outPorts;
    this.subGraphs = subGraphs;
  }

  @Override
  public Set<T> subGraphs() {
    return Collections.unmodifiableSet(subGraphs);
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.unmodifiableList(inPorts);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.unmodifiableList(outPorts);
  }

  @Override
  public Map<OutPort, InPort> downstreams() {
    return Collections.unmodifiableMap(this.downstreams);
  }

  private static void assertCorrectWires(final Set<? extends Graph> graphs,
                                         final Map<OutPort, InPort> wires) {
    wires.forEach((from, to) -> assertCorrectWire(graphs, from, to));
  }

  private static void assertCorrectWire(final Set<? extends Graph> graphs, final OutPort from, final InPort to) {
    if (graphs.stream().noneMatch(graph -> graph.outPorts().contains(from))) {
      throw new WiringException("Out ports of " + graphs + " hasn't got " + from);
    }

    if (graphs.stream().noneMatch(graph -> graph.inPorts().contains(to))) {
      throw new WiringException("In ports of " + graphs + " hasn't got " + to);
    }
  }
}
