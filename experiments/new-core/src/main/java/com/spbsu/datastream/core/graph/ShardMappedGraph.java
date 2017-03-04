package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

public class ShardMappedGraph extends AbstractComposedGraph<AtomicGraph> {
  private final String nodeId;

  public ShardMappedGraph(final FlatGraph flatGraph, final String nodeId) {
    super(flatGraph);
    this.nodeId = nodeId;
  }

  //For deep copy only
  private ShardMappedGraph(
          final Map<InPort, OutPort> upstreams,
          final Map<OutPort, InPort> downstreams, final List<InPort> inPorts,
          final List<OutPort> outPorts, final Set<AtomicGraph> subGraphs, final String nodeId) {
    super(upstreams, downstreams, inPorts, outPorts, subGraphs);
    this.nodeId = nodeId;
  }

  public String nodeId() {
    return nodeId;
  }

  @Override
  public Graph deepCopy() {
    final List<AtomicGraph> subGraphs = new ArrayList<>(subGraphs());
    final List<AtomicGraph> subGraphsCopy = subGraphs.stream().map(Graph::deepCopy)
            .map(AtomicGraph.class::cast)
            .collect(Collectors.toList());
    final Map<InPort, InPort> inPortsMapping = inPortsMapping(subGraphs, subGraphsCopy);
    final Map<OutPort, OutPort> outPortsMapping = outPortsMapping(subGraphs, subGraphsCopy);

    final Map<InPort, OutPort> upstreams = mappedUpstreams(upstreams(), inPortsMapping, outPortsMapping);
    final Map<OutPort, InPort> downstreams = mappedDownstreams(downstreams(), inPortsMapping, outPortsMapping);
    final List<InPort> inPorts = mappedInPorts(inPorts(), inPortsMapping);
    final List<OutPort> outPorts = mappedOutPorts(outPorts(), outPortsMapping);
    return new ShardMappedGraph(upstreams, downstreams, inPorts, outPorts, new HashSet<>(subGraphsCopy), this.nodeId);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    final ShardMappedGraph that = (ShardMappedGraph) o;
    return Objects.equals(nodeId, that.nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), nodeId);
  }

  @Override
  public String toString() {
    return "ShardMappedGraph{" +
            "nodeId=" + nodeId() +
            ", upstreams=" + upstreams() +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
            '}';
  }
}
