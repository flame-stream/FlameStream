package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by marnikitta on 2/8/17.
 */
public abstract class AbstractComposedGraph<T extends Graph> implements ComposedGraph<T> {
  private final Map<InPort, OutPort> upstreams;
  private final Map<OutPort, InPort> downstreams;

  private final List<InPort> inPorts;
  private final List<OutPort> outPorts;

  private final Set<T> subGraphs;

  protected AbstractComposedGraph(final Set<T> graph) {
    this(graph, Collections.emptyMap());
  }

  protected AbstractComposedGraph(final T graph,
                                  final OutPort from,
                                  final InPort to) {
    this(Collections.singleton(graph), Collections.singletonMap(from, to));
  }

  protected AbstractComposedGraph(final Set<T> graphs,
                                  final Map<OutPort, InPort> wires) {
    ComposedGraph.assertCorrectWires(graphs, wires);

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
