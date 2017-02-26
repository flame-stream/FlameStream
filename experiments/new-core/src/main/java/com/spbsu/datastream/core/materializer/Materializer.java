package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.graph.Graph;

/**
 * Created by marnikitta on 2/7/17.
 */
public interface Materializer {
  void materialize(Graph graph);
}
