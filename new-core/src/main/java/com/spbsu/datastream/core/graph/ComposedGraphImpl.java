package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by marnikitta on 2/6/17.
 */
final class ComposedGraphImpl extends AbstractComposedGraph<Graph> {
  ComposedGraphImpl(final Set<Graph> graph) {
    super(graph);
  }

  ComposedGraphImpl(final Graph graph,
                    final OutPort from,
                    final InPort to) {
    super(graph, from, to);
  }

  //for deep copy only
  private ComposedGraphImpl(final Map<InPort, OutPort> upstreams,
                            final Map<OutPort, InPort> downstreams,
                            final List<InPort> inPorts,
                            final List<OutPort> outPorts,
                            final Set<Graph> subGraphs) {
    super(upstreams, downstreams, inPorts, outPorts, subGraphs);
  }

  @Override
  public String toString() {
    return "ComposedGraphImpl{" + "upstreams=" + upstreams() +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
            '}';
  }

  @Override
  public Graph deepCopy() {
    final List<Graph> subGraphs = new ArrayList<>(subGraphs());
    final List<Graph> subGraphsCopy = subGraphs.stream().map(Graph::deepCopy).collect(Collectors.toList());
    final Map<InPort, InPort> inPortsMapping = inPortsMapping(subGraphs, subGraphsCopy);
    final Map<OutPort, OutPort> outPortsMapping = outPortsMapping(subGraphs, subGraphsCopy);

    final Map<InPort, OutPort> upstreams = mappedUpstreams(upstreams(), inPortsMapping, outPortsMapping);
    final Map<OutPort, InPort> downstreams = mappedDownstreams(downstreams(), inPortsMapping, outPortsMapping);
    final List<InPort> inPorts = mappedInPorts(inPorts(), inPortsMapping);
    final List<OutPort> outPorts = mappedOutPorts(outPorts(), outPortsMapping);

    return new ComposedGraphImpl(upstreams, downstreams, inPorts, outPorts, new HashSet<>(subGraphsCopy));
  }
}
