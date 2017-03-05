package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class FlatGraph extends AbstractComposedGraph<AtomicGraph> {
  private FlatGraph(final Set<AtomicGraph> subGraphs,
                    final Map<OutPort, InPort> wires) {
    super(subGraphs, wires);
  }

  //for deep copy only
  private FlatGraph(final Map<InPort, OutPort> upstreams,
                    final Map<OutPort, InPort> downstreams,
                    final List<InPort> inPorts,
                    final List<OutPort> outPorts,
                    final Set<AtomicGraph> subGraphs) {
    super(upstreams, downstreams, inPorts, outPorts, subGraphs);
  }

  public static FlatGraph flattened(final Graph graph) {
    if (graph instanceof FlatGraph) {
      return (FlatGraph) graph;
    } else if (graph instanceof AtomicGraph) {
      return new FlatGraph(Collections.singleton((AtomicGraph) graph), Collections.emptyMap());
    } else if (graph instanceof ComposedGraph) {
      final ComposedGraphImpl composed = (ComposedGraphImpl) graph;

      final Set<FlatGraph> flatteneds = composed.subGraphs().stream()
              .map(FlatGraph::flattened).collect(Collectors.toSet());
      final Set<AtomicGraph> atomics = flatteneds.stream().map(FlatGraph::subGraphs)
              .flatMap(Set::stream).collect(Collectors.toSet());

      final Map<OutPort, InPort> downstreams = flatteneds.stream().map(FlatGraph::downstreams)
              .map(Map::entrySet).flatMap(Set::stream).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      downstreams.putAll(composed.downstreams());

      return new FlatGraph(atomics, downstreams);
    } else {
      throw new UnsupportedOperationException("Unsupported graph type");
    }
  }

  @Override
  public String toString() {
    return "FlatGraph{" + "upstreams=" + upstreams() +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
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

    return new FlatGraph(upstreams, downstreams, inPorts, outPorts, new HashSet<>(subGraphsCopy));
  }
}
