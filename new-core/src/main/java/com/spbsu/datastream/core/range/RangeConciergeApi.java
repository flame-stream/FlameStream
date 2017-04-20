package com.spbsu.datastream.core.range;

import com.spbsu.datastream.core.graph.TheGraph;

public interface RangeConciergeApi {
  final class DeployForTick {
    private final TheGraph graph;

    private final long tick;

    public DeployForTick(final TheGraph graph, final long tick) {
      this.graph = graph;
      this.tick = tick;
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
              '}';
    }
  }
}
