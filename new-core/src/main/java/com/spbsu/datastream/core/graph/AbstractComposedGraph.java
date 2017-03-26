package com.spbsu.datastream.core.graph;

import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractComposedGraph<T extends Graph> implements ComposedGraph<T> {
  private final Map<InPort, OutPort> upstreams;
  private final Map<OutPort, InPort> downstreams;

  private final List<InPort> inPorts;
  private final List<OutPort> outPorts;

  private final Set<T> subGraphs;

  AbstractComposedGraph(final AbstractComposedGraph<T> that) {
    this(that.upstreams, that.downstreams, that.inPorts, that.outPorts, that.subGraphs);
  }

  AbstractComposedGraph(final Set<T> graphs) {
    this(graphs, Collections.emptyMap());
  }

  AbstractComposedGraph(final T graph,
                        final OutPort from,
                        final InPort to) {
    this(Collections.singleton(graph), Collections.singletonMap(from, to));
  }

  AbstractComposedGraph(final Set<T> graphs,
                        final Map<OutPort, InPort> wires) {
    assertCorrectWires(graphs, wires);

    this.subGraphs = new HashSet<>(graphs);

    this.downstreams = new HashMap<>(wires);
    this.upstreams = wires.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    this.inPorts = graphs.stream().map(Graph::inPorts)
            .flatMap(Collection::stream).filter(port -> !upstreams.containsKey(port))
            .collect(Collectors.toList());

    this.outPorts = graphs.stream().map(Graph::outPorts)
            .flatMap(List::stream).filter(port -> !downstreams.containsKey(port))
            .collect(Collectors.toList());
  }

  /**
   * Only for deep copying
   */
  AbstractComposedGraph(final Map<InPort, OutPort> upstreams,
                        final Map<OutPort, InPort> downstreams,
                        final List<InPort> inPorts,
                        final List<OutPort> outPorts,
                        final Set<T> subGraphs) {
    this.upstreams = upstreams;
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

  @Override
  public Map<InPort, OutPort> upstreams() {
    return Collections.unmodifiableMap(this.upstreams);
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final AbstractComposedGraph<?> that = (AbstractComposedGraph<?>) o;
    return Objects.equals(upstreams, that.upstreams) &&
            Objects.equals(downstreams, that.downstreams) &&
            Objects.equals(inPorts, that.inPorts) &&
            Objects.equals(outPorts, that.outPorts) &&
            Objects.equals(subGraphs, that.subGraphs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(upstreams, downstreams, inPorts, outPorts, subGraphs);
  }

  @Override
  public String toString() {
    return "AbstractComposedGraph{" + "upstreams=" + upstreams +
            ", downstreams=" + downstreams +
            ", inPorts=" + inPorts +
            ", outPorts=" + outPorts +
            ", subGraphs=" + subGraphs +
            '}';
  }
}
