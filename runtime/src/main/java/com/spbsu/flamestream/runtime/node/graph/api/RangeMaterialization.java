package com.spbsu.flamestream.runtime.node.graph.api;

import akka.actor.ActorRef;
import org.apache.commons.lang.math.IntRange;

public class RangeMaterialization {
  private final IntRange range;
  private final ActorRef materialization;

  public RangeMaterialization(IntRange range, ActorRef materialization) {
    this.range = range;
    this.materialization = materialization;
  }

  public IntRange range() {
    return range;
  }

  public ActorRef materialization() {
    return materialization;
  }

  @Override
  public String toString() {
    return "RangeMaterialization{" +
            "range=" + range +
            ", materialization=" + materialization +
            '}';
  }
}
