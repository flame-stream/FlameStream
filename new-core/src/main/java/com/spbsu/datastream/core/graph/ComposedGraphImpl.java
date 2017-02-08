package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by marnikitta on 2/6/17.
 */
final class ComposedGraphImpl extends AbstractComposedGraph<Graph> {
  ComposedGraphImpl(final Set<Graph> graph) {
    this(graph, Collections.emptyMap());
  }

  ComposedGraphImpl(final Graph graph,
                    final OutPort from,
                    final InPort to) {
    super(graph, from, to);
  }

  private ComposedGraphImpl(final Set<Graph> graphs,
                            final Map<OutPort, InPort> wires) {
    super(graphs, wires);
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
