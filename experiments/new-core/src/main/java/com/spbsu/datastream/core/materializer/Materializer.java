package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.graph.ShardMappedGraph;

public interface Materializer {
  void materialize(ShardMappedGraph graph);
}
