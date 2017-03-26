package com.spbsu.datastream.core.graph;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
}
