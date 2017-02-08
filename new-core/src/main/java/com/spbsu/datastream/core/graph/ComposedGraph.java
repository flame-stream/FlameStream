package com.spbsu.datastream.core.graph;

import java.util.Map;
import java.util.Set;

/**
 * Created by marnikitta on 2/8/17.
 */
public interface ComposedGraph<T extends Graph> extends Graph {
  Set<T> subGraphs();

  Map<OutPort, InPort> downstreams();

  Map<InPort, OutPort> upstreams();

  static void assertCorrectWires(final Set<? extends Graph> graphs,
                                 final Map<OutPort, InPort> wires) {
    wires.forEach((from, to) -> assertCorrectWire(graphs, from, to));
  }

  static void assertCorrectWire(final Set<? extends Graph> graphs, final OutPort from, final InPort to) {
    if (!graphs.stream().anyMatch(graph -> graph.outPorts().contains(from))) {
      throw new WiringException();
    }

    if (!graphs.stream().anyMatch(graph -> graph.inPorts().contains(to))) {
      throw new WiringException();
    }
  }
}
