package com.spbsu.datastream.core.graph;

import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

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

  AbstractComposedGraph(final Set<T> graph) {
    this(graph, Collections.emptyMap());
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

  static <T extends Graph> Map<InPort, InPort> inPortsMapping(final List<T> graphs,
                                                              final List<T> graphsCopy) {
    return Seq.zip(graphs.stream().map(Graph::inPorts).flatMap(Collection::stream),
            graphsCopy.stream().map(Graph::inPorts).flatMap(Collection::stream))
            .collect(Collectors.toMap(Tuple2::v1, Tuple2::v2));
  }

  static <T extends Graph> Map<OutPort, OutPort> outPortsMapping(final List<T> graphs,
                                                                 final List<T> graphsCopy) {
    return Seq.zip(graphs.stream().map(Graph::outPorts).flatMap(Collection::stream),
            graphsCopy.stream().map(Graph::outPorts).flatMap(Collection::stream))
            .collect(Collectors.toMap(Tuple2::v1, Tuple2::v2));
  }

  static Map<InPort, OutPort> mappedUpstreams(final Map<InPort, OutPort> upstreams,
                                              final Map<InPort, InPort> inPortsMapping,
                                              final Map<OutPort, OutPort> outPortsMapping) {
    return upstreams.entrySet().stream()
            .collect(Collectors.toMap((e) -> inPortsMapping.get(e.getKey()), (e) -> outPortsMapping.get(e.getValue())));
  }

  static Map<OutPort, InPort> mappedDownstreams(final Map<OutPort, InPort> downstreams,
                                                final Map<InPort, InPort> inPortsMapping,
                                                final Map<OutPort, OutPort> outPortsMapping) {
    return downstreams.entrySet().stream()
            .collect(Collectors.toMap((e) -> outPortsMapping.get(e.getKey()), (e) -> inPortsMapping.get(e.getValue())));
  }

  static List<InPort> mappedInPorts(final List<InPort> inPorts,
                                    final Map<InPort, InPort> inPortsMapping) {
    return inPorts.stream().map(inPortsMapping::get).collect(Collectors.toList());
  }

  static List<OutPort> mappedOutPorts(final List<OutPort> outPorts,
                                      final Map<OutPort, OutPort> outPortsMapping) {
    return outPorts.stream().map(outPortsMapping::get).collect(Collectors.toList());
  }

  private static void assertCorrectWires(final Set<? extends Graph> graphs,
                                         final Map<OutPort, InPort> wires) {
    wires.forEach((from, to) -> assertCorrectWire(graphs, from, to));
  }

  @SuppressWarnings("SimplifyStreamApiCallChains")
  private static void assertCorrectWire(final Set<? extends Graph> graphs, final OutPort from, final InPort to) {
    if (!graphs.stream().anyMatch(graph -> graph.outPorts().contains(from))) {
      throw new WiringException();
    }

    if (!graphs.stream().anyMatch(graph -> graph.inPorts().contains(to))) {
      throw new WiringException();
    }
  }
}
