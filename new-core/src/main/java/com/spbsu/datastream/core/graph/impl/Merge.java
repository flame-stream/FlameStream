package com.spbsu.datastream.core.graph.impl;

import com.spbsu.datastream.core.graph.FanIn;

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
}
