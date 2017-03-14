package com.spbsu.datastream.core.deploy;

import com.spbsu.datastream.core.graph.TheGraph;

public interface DeployApi {
  class DeployForTick {
    private final TheGraph graph;

    private final long tick;

    public DeployForTick(final TheGraph graph, final long tick) {
      this.graph = graph;
      this.tick = tick;
    }

    public TheGraph graph() {
      return graph;
    }

    public long tick() {
      return tick;
    }

    @Override
    public String toString() {
      return "DeployForTick{" + "graph=" + graph +
              ", tick=" + tick +
              '}';
    }
  }
}
