package com.spbsu.datastream.core.node;

import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

public final class DeployForTick {
  private final TheGraph graph;

  private final long tick;

  private final long startTs;

  private final long window;

  private final HashRange ackerLocation;

  public DeployForTick(final TheGraph graph,
                       final HashRange ackerLocation,
                       final long tick,
                       final long startTs,
                       final long window) {
    this.ackerLocation = ackerLocation;
    this.graph = graph;
    this.tick = tick;
    this.startTs = startTs;
    this.window = window;
  }

  public HashRange ackerRange() {
    return this.ackerLocation;
  }

  public long window() {
    return this.window;
  }

  public long startTs() {
    return this.startTs;
  }

  public TheGraph graph() {
    return this.graph;
  }

  public long tick() {
    return this.tick;
  }

  @Override
  public String toString() {
    return "DeployForTick{" + "graph=" + this.graph +
            ", tick=" + this.tick +
            ", startTs=" + this.startTs +
            ", window=" + this.window +
            '}';
  }
}