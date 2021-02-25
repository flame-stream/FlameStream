package com.spbsu.flamestream.example.nexmark;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.example.labels.Materializer;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Map;
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
            )).acking(SystemConfig.Acking.DISABLED)
            .build()) {
      try (final FlameRuntime.Flame flame = runtime.run(graph)) {
        final var rears =
                flame.attachRear("rear", new TimingsRearType<>(new SimpleRearType(), 10)).collect(Collectors.toList());

        flame.attachFront("front", new GeneratorFrontType(nexmarkConfiguration, Map.ofEntries(
                Map.entry(new EdgeId("front", "node-0"), 0),
                Map.entry(new EdgeId("front", "node-1"), 1),
                Map.entry(new EdgeId("front", "node-2"), 2),
                Map.entry(new EdgeId("front", "node-3"), 3)
        ), currentTimeMillis, 10000));
        for (final var rear : rears) {
          rear.await();
        }
      }
    }
  }
}
