package com.spbsu.datastream.core.range;

import com.spbsu.datastream.core.graph.TheGraph;

public interface RangeConciergeApi {
  final class DeployForTick {
    private final TheGraph graph;

    private final long tick;

    private final long startTs;

    private final long window;

    public DeployForTick(final TheGraph graph, final long tick, final long startTs, final long window) {
      this.graph = graph;
      this.tick = tick;
      this.startTs = startTs;
      this.window = window;
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
}
