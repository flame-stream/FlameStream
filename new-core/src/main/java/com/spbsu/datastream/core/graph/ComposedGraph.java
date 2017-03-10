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
}
