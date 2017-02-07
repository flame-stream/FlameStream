package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class FlattenedGraph implements Graph {
  private final Set<AtomicGraph> parts;
  private final Set<InPort> inPorts;
  private final Set<OutPort> outPorts;
  private final Map<OutPort, InPort> downstreams;
  private final Map<InPort, OutPort> upstreams;

  public static FlattenedGraph flattened(final Graph graph) {
    if (graph instanceof FlattenedGraph) {
      return (FlattenedGraph) graph;
    } else if (graph instanceof AtomicGraph) {
      return new FlattenedGraph(Collections.singleton((AtomicGraph) graph), Collections.emptyMap());
    } else if (graph instanceof ComposedGraph) {
      final ComposedGraph composed = (ComposedGraph) graph;

      final Set<FlattenedGraph> flatteneds = composed.parts().stream()
              .map(FlattenedGraph::flattened).collect(Collectors.toSet());
      final Set<AtomicGraph> atomics = flatteneds.stream().map(FlattenedGraph::parts)
              .flatMap(Set::stream).collect(Collectors.toSet());

      final Map<OutPort, InPort> downstreams = flatteneds.stream().map(FlattenedGraph::downstreams)
              .map(Map::entrySet).flatMap(Set::stream).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      downstreams.putAll(composed.downstreams());

      return new FlattenedGraph(atomics, downstreams);
    } else {
      throw new UnsupportedOperationException("Unsupported graph type");
    }
  }

  private FlattenedGraph(final Set<AtomicGraph> parts,
                         final Map<OutPort, InPort> wires) {
    ComposedGraph.assertCorrectWires(parts, wires);

    this.parts = new HashSet<>(parts);
    this.downstreams = new HashMap<>(wires);
    this.upstreams = wires.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    this.inPorts = parts.stream().map(AtomicGraph::inPorts).flatMap(Set::stream)
            .filter(port -> !this.upstreams.containsKey(port)).collect(Collectors.toSet());
    this.outPorts = parts.stream().map(AtomicGraph::outPorts).flatMap(Set::stream)
            .filter(port -> !this.downstreams.containsKey(port)).collect(Collectors.toSet());

  }

  public Set<AtomicGraph> parts() {
    return parts;
  }

  @Override
  public Set<InPort> inPorts() {
    return Collections.unmodifiableSet(inPorts);
  }

  @Override
  public Set<OutPort> outPorts() {
    return Collections.unmodifiableSet(outPorts);
  }

  @Override
  public Map<OutPort, InPort> downstreams() {
    return Collections.unmodifiableMap(downstreams);
  }

  @Override
  public Map<InPort, OutPort> upstreams() {
    return Collections.unmodifiableMap(upstreams);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final FlattenedGraph that = (FlattenedGraph) o;
    return Objects.equals(parts, that.parts) &&
            Objects.equals(inPorts, that.inPorts) &&
            Objects.equals(outPorts, that.outPorts) &&
            Objects.equals(downstreams, that.downstreams) &&
            Objects.equals(upstreams, that.upstreams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parts, inPorts, outPorts, downstreams, upstreams);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("FlattenedGraph{");
    sb.append("parts=").append(parts);
    sb.append(", inPorts=").append(inPorts);
    sb.append(", outPorts=").append(outPorts);
    sb.append(", downstreams=").append(downstreams);
    sb.append(", upstreams=").append(upstreams);
    sb.append('}');
    return sb.toString();
  }
}
