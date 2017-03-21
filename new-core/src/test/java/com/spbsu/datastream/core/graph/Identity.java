package com.spbsu.datastream.core.graph;

public final class Identity extends Processor {
  @Override
  public Graph deepCopy() {
    return new Identity();
  }
}
