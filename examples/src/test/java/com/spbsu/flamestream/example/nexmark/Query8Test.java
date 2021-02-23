package com.spbsu.flamestream.example.nexmark;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.spbsu.flamestream.example.labels.Materializer;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.stream.Collectors;

public class Query8Test extends FlameAkkaSuite {
  @Test
  public void test() throws Exception {
    var nexmarkConfiguration = new NexmarkConfiguration();
    nexmarkConfiguration.bidProportion = 1;
    nexmarkConfiguration.personProportion = 1;
    nexmarkConfiguration.auctionProportion = 2;
    nexmarkConfiguration.firstEventRate /= 1;
    nexmarkConfiguration.nextEventRate /= 1;
    nexmarkConfiguration.windowSizeSec *= 1;

    final var graph = Materializer.materialize(Query8.create(nexmarkConfiguration.windowSizeSec));

    final var currentTimeMillis = System.currentTimeMillis();
    try (final var runtime = new LocalRuntime.Builder()
            .millisBetweenCommits(Integer.MAX_VALUE)
            .defaultMinimalTime(Query8.tumbleStart(
                    Instant.ofEpochMilli(currentTimeMillis),
                    nexmarkConfiguration.windowSizeSec
            ))
            .build()) {
      try (final FlameRuntime.Flame flame = runtime.run(graph)) {
        final var rears = flame.attachRear("rear", new LatencyRearType()).collect(Collectors.toList());

        final var handles = flame
                .attachFront("front", new GeneratorFrontType(nexmarkConfiguration))
                .collect(Collectors.toList());
        for (final var handle : handles.subList(1, handles.size())) {
          handle.unregister();
        }
        handles.get(0).generate();
        for (final var rear : rears) {
          rear.await();
        }
      }
    }
  }
}
