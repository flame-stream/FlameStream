package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.materializer.GraphStageLogic;

/**
 * AtomicGraph - graph without inner down or upstreams
 */
public interface AtomicGraph extends Graph {
  GraphStageLogic logic();
}
