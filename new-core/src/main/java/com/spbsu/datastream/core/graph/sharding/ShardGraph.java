package com.spbsu.datastream.core.graph.sharding;

import com.spbsu.datastream.core.graph.AbstractComposedGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.PhysicalGraph;

import java.util.Map;
import java.util.Set;

/**
 * Created by marnikitta on 2/8/17.
 */
public class ShardGraph extends AbstractComposedGraph<PhysicalGraph> {
  protected ShardGraph(final Set<PhysicalGraph> graph) {
    super(graph);
  }

  protected ShardGraph(final PhysicalGraph graph, final OutPort from, final InPort to) {
    super(graph, from, to);
  }

  protected ShardGraph(final Set<PhysicalGraph> graphs, final Map<OutPort, InPort> wires) {
    super(graphs, wires);
  }
}
