package com.spbsu.flamestream.example.nexmark;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.generator.NexmarkGenerator;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.example.labels.Materializer;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.SimpleFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
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

    final Graph graph = Materializer.materialize(Query8.create(nexmarkConfiguration.windowSizeSec));

    final var currentTimeMillis = System.currentTimeMillis();
    try (final LocalRuntime runtime = new LocalRuntime.Builder()
            .millisBetweenCommits(Integer.MAX_VALUE)
            .defaultMinimalTime(Query8.tumbleStart(Instant.ofEpochMilli(currentTimeMillis), nexmarkConfiguration.windowSizeSec))
            .build()) {
      try (final FlameRuntime.Flame flame = runtime.run(graph)) {
        final AwaitResultConsumer<Query8.PersonGroupingKey> awaitConsumer =
                new AwaitResultConsumer<>(1);
        flame.attachRear("rear", new AkkaRearType<>(runtime.system(), Object.class))
                .forEach(r -> r.addListener(x -> {}));

        final var handles = flame
                .attachFront("front", new SimpleFront.Type())
                .collect(Collectors.toList());
        for (final var handle : handles.subList(1, handles.size())) {
          System.out.println("unregister");
          handle.unregister();
        }
        final var generator = new NexmarkGenerator(new GeneratorConfig(
                nexmarkConfiguration,
                currentTimeMillis,
                1,
                100000,
                1
        ));
        while (generator.hasNext()) {
          final var nextEvent = generator.nextEvent();
          final var event = nextEvent.event;
          final var instant = Instant.ofEpochMilli(nextEvent.eventTimestamp);
          final var sleep = Duration.between(Instant.now(), instant);
          if (!sleep.isNegative()) {
            Thread.sleep(sleep.toMillis());
          }
          handles.get(0).onDataItem(Query8.tumbleStart(instant, nexmarkConfiguration.windowSizeSec), event);
        }
        handles.get(0).unregister();

        awaitConsumer.await(20000, TimeUnit.SECONDS);
      }
    }
  }
}
