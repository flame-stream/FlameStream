package com.spbsu.datastream.core.graph;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

public class ShardMappedGraph extends AbstractComposedGraph<AtomicGraph> {
  private final InetSocketAddress address;

  public ShardMappedGraph(final FlatGraph flatGraph, final InetSocketAddress address) {
    super(flatGraph);
    this.address = address;
  }

  //For deep copy only
  private ShardMappedGraph(
          final Map<InPort, OutPort> upstreams,
          final Map<OutPort, InPort> downstreams, final List<InPort> inPorts,
          final List<OutPort> outPorts, final Set<AtomicGraph> subGraphs, final InetSocketAddress address) {
    super(upstreams, downstreams, inPorts, outPorts, subGraphs);
    this.address = address;
  }

  public InetSocketAddress address() {
    return address;
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
    return new ShardMappedGraph(upstreams, downstreams, inPorts, outPorts, new HashSet<>(subGraphsCopy), this.address);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    final ShardMappedGraph that = (ShardMappedGraph) o;
    return Objects.equals(address, that.address);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), address);
  }

  @Override
  public String toString() {
    return "ShardMappedGraph{" +
            "address=" + address() +
            ", upstreams=" + upstreams() +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
            '}';
  }
}
