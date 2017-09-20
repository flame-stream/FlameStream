package com.spbsu.datastream.core.graph;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class ComposedGraphImpl<T extends Graph> implements ComposedGraph<T> {
  private final Map<OutPort, InPort> downstreams;

  private final List<InPort> inPorts;
  private final List<OutPort> outPorts;

  private final Set<T> subGraphs;

  public ComposedGraphImpl(Set<T> graphs) {
    this(graphs, Collections.emptyMap());
  }

  ComposedGraphImpl(T graph,
                    OutPort from,
                    InPort to) {
    this(Collections.singleton(graph), Collections.singletonMap(from, to));
  }

  private ComposedGraphImpl(Set<T> graphs,
                            Map<OutPort, InPort> wires) {
    ComposedGraphImpl.assertCorrectWires(graphs, wires);

    this.subGraphs = new HashSet<>(graphs);

    this.downstreams = new HashMap<>(wires);

    this.inPorts = graphs.stream().map(Graph::inPorts)
            .flatMap(Collection::stream).filter(port -> !downstreams.containsValue(port))
            .collect(Collectors.toList());

    this.outPorts = graphs.stream().map(Graph::outPorts)
            .flatMap(List::stream).filter(port -> !downstreams.containsKey(port))
            .collect(Collectors.toList());
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
  public ComposedGraph<AtomicGraph> flattened() {
    final Set<ComposedGraph<AtomicGraph>> flatteneds = subGraphs().stream()
            .map(Graph::flattened).collect(Collectors.toSet());
    final Set<AtomicGraph> atomics = flatteneds.stream().map(ComposedGraph::subGraphs)
            .flatMap(Set::stream).collect(Collectors.toSet());

    final Map<OutPort, InPort> flatDownStreams = flatteneds.stream().map(ComposedGraph::downstreams)
            .map(Map::entrySet).flatMap(Set::stream).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    flatDownStreams.putAll(downstreams());

    return new ComposedGraphImpl<>(atomics, flatDownStreams);
  }

  @Override
  public Map<OutPort, InPort> downstreams() {
    return Collections.unmodifiableMap(downstreams);
  }

  private static void assertCorrectWires(Collection<? extends Graph> graphs,
                                         Map<OutPort, InPort> wires) {
    wires.forEach((from, to) -> ComposedGraphImpl.assertCorrectWire(graphs, from, to));
  }

  private static void assertCorrectWire(Collection<? extends Graph> graphs, OutPort from, InPort to) {
    if (graphs.stream().noneMatch(graph -> graph.outPorts().contains(from))) {
      throw new WiringException("Out ports of " + graphs + " hasn't got " + from);
    }

    if (graphs.stream().noneMatch(graph -> graph.inPorts().contains(to))) {
      throw new WiringException("In ports of " + graphs + " hasn't got " + to);
    }
  }
}
