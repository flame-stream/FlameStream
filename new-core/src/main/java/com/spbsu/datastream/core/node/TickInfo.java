package com.spbsu.datastream.core.node;

import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

public final class TickInfo {
  private final TheGraph graph;

  private final long startTs;

  private final long stopTs;

  private final long window;

  private final HashRange ackerLocation;

  public TickInfo(final TheGraph graph,
                  final HashRange ackerLocation,
                  final long startTs,
                  final long stopTs,
                  final long window) {
    this.ackerLocation = ackerLocation;
    this.graph = graph;
    this.startTs = startTs;
    this.window = window;
    this.stopTs = stopTs;
  }

  public long stopTs() {
    return this.stopTs;
  }

  public HashRange ackerRange() {
    return this.ackerLocation;
  }

  public long window() {
    return this.window;
  }

  public TheGraph graph() {
    return this.graph;
  }

  public long startTs() {
    return this.startTs;
  }

  @Override
  public String toString() {
    return "TickInfo{" + "graph=" + this.graph +
            ", startTs=" + this.startTs +
            ", stopTs=" + this.stopTs +
            ", window=" + this.window +
            ", ackerLocation=" + this.ackerLocation +
            '}';
  }
}