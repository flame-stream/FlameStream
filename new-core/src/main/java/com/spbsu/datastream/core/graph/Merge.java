package com.spbsu.datastream.core.graph;

/**
 * Created by marnikitta on 2/7/17.
 */
public class Merge extends FanIn {
  public Merge(final int n) {
    super(n);
  }

  @Override
  public String toString() {
    return "Merge{" + super.toString() + '}';
  }
}
