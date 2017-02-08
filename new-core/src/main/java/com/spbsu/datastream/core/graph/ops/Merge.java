package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.graph.FanIn;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Merge extends FanIn {
  public Merge(final int n) {
    super(n);
  }

  @Override
  public String toString() {
    return "Merge{" + super.toString() + '}';
  }

  @Override
  public GraphStageLogic logic() {
    return null;
  }
}
