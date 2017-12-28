package com.spbsu.flamestream.example.benchmark;

import com.spbsu.flamestream.example.bl.index.InvertedIndexGraph;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class LocalGraphDeployer implements GraphDeployer {
  private final LocalRuntime runtime;

  public LocalGraphDeployer(int parallelism, int maxElementsInGraph) {
    runtime = new LocalRuntime(parallelism, maxElementsInGraph);
  }

  @Override
  public void deploy() {
    final FlameRuntime.Flame flame = runtime.run(new InvertedIndexGraph().get());
    // TODO: 28.12.2017 attach front & rear
  }

  @Override
  public void stop() {
    runtime.close();
  }
}
