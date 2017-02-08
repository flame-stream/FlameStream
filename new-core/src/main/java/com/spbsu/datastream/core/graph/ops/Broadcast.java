package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.FanOut;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Broadcast extends FanOut implements AtomicGraph {
  public Broadcast(final int shape) {
    super(shape);
  }

  @Override
  public String toString() {
    return "Broadcast{" + super.toString() + '}';
  }

  @Override
  public GraphStageLogic logic() {
    return null;
  }
}
