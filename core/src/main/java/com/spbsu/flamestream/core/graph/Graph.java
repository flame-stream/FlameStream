package com.spbsu.flamestream.core.graph;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public interface Graph {
  List<InPort> inPorts();

  List<OutPort> outPorts();

  ComposedGraph<AtomicGraph> flattened();

  /**
   * Fuses this Graph to `that` Graph by wiring together `from` and `to`.
   *
   * @param that a Graph to fuse with
   * @param from the data source to wire
   * @param to   the data sink to wire
   * @return a Graph representing the fusion of `this` and `that`
   **/

  Graph fuse(Graph that, OutPort from, InPort to);

  /**
   * Creates a new Graph which is `this` Graph composed with `that` Graph.
   *
   * @param that a Graph to be composed with (cannot be itself)
   * @return a Graph that represents the composition of `this` and `that`
   **/

  Graph compose(Graph that);

  /**
   * Creates a new Graph based on the current Graph but with
   * the given OutPort wired to the given InPort.
   *
   * @param from the OutPort to wire
   * @param to   the InPort to wire
   * @return a new Graph with the ports wired
   */
  Graph wire(OutPort from, InPort to);
}
