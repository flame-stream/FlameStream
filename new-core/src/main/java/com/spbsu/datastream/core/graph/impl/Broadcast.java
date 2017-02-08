package com.spbsu.datastream.core.graph.impl;

import com.spbsu.datastream.core.graph.FanOut;
import com.spbsu.datastream.core.graph.PhysicalGraph;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Broadcast extends FanOut implements PhysicalGraph {
  public Broadcast(final int shape) {
    super(shape);
  }

  @Override
  public String toString() {
    return "Broadcast{" + super.toString() + '}';
  }
}
