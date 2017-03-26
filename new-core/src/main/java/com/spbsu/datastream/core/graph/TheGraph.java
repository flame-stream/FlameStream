package com.spbsu.datastream.core.graph;

import java.util.*;
import java.util.stream.Collectors;

public class TheGraph extends AbstractComposedGraph<AtomicGraph> {
  public TheGraph(final FlatGraph flatGraph) {
    super(flatGraph);
  }

  //For deep copy only
  private TheGraph(
          final Map<InPort, OutPort> upstreams,
          final Map<OutPort, InPort> downstreams, final List<InPort> inPorts,
          final List<OutPort> outPorts,
          final Set<AtomicGraph> subGraphs) {
    super(upstreams, downstreams, inPorts, outPorts, subGraphs);
  }

  @Override
  public String toString() {
    return "TheGraph{" + "upstreams=" + upstreams() +
            ", downstreams=" + downstreams() +
            ", inPorts=" + inPorts() +
            ", outPorts=" + outPorts() +
            ", subGraphs=" + subGraphs() +
            '}';
  }
}
