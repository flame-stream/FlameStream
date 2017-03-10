package com.spbsu.datastream.core.graph;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Identity extends Processor {
  @Override
  public Graph deepCopy() {
    return new Identity();
  }
}
