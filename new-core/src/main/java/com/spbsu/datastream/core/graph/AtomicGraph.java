package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.materializer.GraphStageLogic;

/**
 * Created by marnikitta on 2/7/17.
 */


/**
 * AtomicGraph - graph without inner down or upstreams
 */
public interface AtomicGraph extends Graph {
  GraphStageLogic logic();
}
