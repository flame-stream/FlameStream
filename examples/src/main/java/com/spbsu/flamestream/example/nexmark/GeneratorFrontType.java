package com.spbsu.flamestream.example.nexmark;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.generator.NexmarkGenerator;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.SimpleFront;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GeneratorFrontType implements FlameRuntime.FrontType<SimpleFront, GeneratorFrontType.Handle> {
  private final SimpleFront.Type base = new SimpleFront.Type();
  private final NexmarkConfiguration nexmarkConfiguration;

  public GeneratorFrontType(NexmarkConfiguration nexmarkConfiguration) {this.nexmarkConfiguration = nexmarkConfiguration;}

  @Override
  public FlameRuntime.FrontInstance<SimpleFront> instance() {
    return base.instance();
  }

  @Override
  public Handle handle(EdgeContext context) {
    return new Handle(base.handle(context), nexmarkConfiguration);
  }

  public static class Handle {
    private final SimpleFront.Handle base;
    private final NexmarkConfiguration nexmarkConfiguration;

    public Handle(SimpleFront.Handle base, NexmarkConfiguration nexmarkConfiguration) {
      this.base = base;
      this.nexmarkConfiguration = nexmarkConfiguration;
    }

    public void generate() throws InterruptedException {
      final var currentTimeMillis = System.currentTimeMillis();
      final var generator = new NexmarkGenerator(new GeneratorConfig(
              nexmarkConfiguration,
              currentTimeMillis,
              1,
              1000000,
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
        base.onDataItem(Query8.tumbleStart(instant, nexmarkConfiguration.windowSizeSec), event);
      }
      base.unregister();
    }

    public void unregister() {
      base.unregister();
    }
  }
}
