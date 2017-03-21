package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.HashFunction;

import java.util.*;
import java.util.stream.Collectors;

public class TheGraph extends AbstractComposedGraph<AtomicGraph> {
  private final Map<InPort, HashFunction<?>> portHashing;

  public TheGraph(final FlatGraph flatGraph) {
    this(flatGraph, Collections.emptyMap());
  }

  public TheGraph(final FlatGraph flatGraph, final Map<InPort, HashFunction<?>> portHashing) {
    super(flatGraph);
    this.portHashing = new HashMap<>(portHashing);
  }

  //For deep copy only
  private TheGraph(
          final Map<InPort, OutPort> upstreams,
          final Map<OutPort, InPort> downstreams, final List<InPort> inPorts,
          final List<OutPort> outPorts,
          final Set<AtomicGraph> subGraphs,
          final Map<InPort, HashFunction<?>> portHashing) {
    super(upstreams, downstreams, inPorts, outPorts, subGraphs);
    this.portHashing = new HashMap<>(portHashing);
  }

  public Map<InPort, HashFunction<?>> portHashing() {
    return portHashing;
  }

  @Override
  public String toString() {
    return "TheGraph{" + "upstreams=" + upstreams() +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
            ", portHashing=" + portHashing() +
            '}';
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

    return new TheGraph(upstreams, downstreams, inPorts, outPorts, new HashSet<>(subGraphsCopy), portHashing());
  }
}
