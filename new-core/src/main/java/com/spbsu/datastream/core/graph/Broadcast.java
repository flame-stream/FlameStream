package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.graph.FanOut;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Broadcast extends FanOut {
  public Broadcast(final int shape) {
    super(shape);
  }

  @Override
  public String toString() {
    return "Broadcast{" + super.toString() + '}';
  }
}
