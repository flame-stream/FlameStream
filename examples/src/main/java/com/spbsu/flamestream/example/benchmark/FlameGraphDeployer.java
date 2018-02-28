package com.spbsu.flamestream.example.benchmark;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.FlameRuntime;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class FlameGraphDeployer implements GraphDeployer {
  private final FlameRuntime runtime;
  private final Graph graph;
  private final FlameRuntime.FrontType<?, ?> frontType;
  private final FlameRuntime.RearType<?, ?> rearType;

  public FlameGraphDeployer(FlameRuntime runtime,
                            Graph graph,
                            FlameRuntime.FrontType<?, ?> frontType,
                            FlameRuntime.RearType<?, ?> rearType) {
    this.runtime = runtime;
    this.graph = graph;
    this.frontType = frontType;
    this.rearType = rearType;
  }

  @Override
  public void deploy() {
    final FlameRuntime.Flame flame = runtime.run(graph);
    flame.attachRear("FlameSocketGraphDeployerRear", rearType);
    flame.attachFront("FlameSocketGraphDeployerFront", frontType);
  }

  @Override
  public void close() {
    try {
      runtime.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
