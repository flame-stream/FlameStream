package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FilterAcceptanceTest extends FlameAkkaSuite {

  private static Graph multiGraph() {
    final Source source = new Source();
    final FlameMap<Integer, Integer> filter1 = new FlameMap<>(new HumbleFiler(-1), Integer.class);
    final FlameMap<Integer, Integer> filter2 = new FlameMap<>(new HumbleFiler(-2), Integer.class);
    final FlameMap<Integer, Integer> filter3 = new FlameMap<>(new HumbleFiler(-3), Integer.class);
    final FlameMap<Integer, Integer> filter4 = new FlameMap<>(new HumbleFiler(-4), Integer.class);
    final Sink sink = new Sink();

    return new Graph.Builder().link(source, filter1)
            .link(filter1, filter2)
            .link(filter2, filter3)
            .link(filter3, filter4)
            .link(filter4, sink)
            .build(source, sink);
  }

  @Test
  public void linearFilter() throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime(DEFAULT_PARALLELISM)) {
      final FlameRuntime.Flame flame = runtime.run(multiGraph());
      {
        final List<AkkaFrontType.Handle<Integer>> handles = flame
                .attachFront("linearFilterFront", new AkkaFrontType<Integer>(runtime.system(), false))
                .collect(Collectors.toList());
        final int streamSize = 10000;
        final Queue<Integer> source = new Random()
                .ints(streamSize)
                .boxed()
                .limit(streamSize)
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        final Set<Integer> expected = source
                .stream()
                .map(integer -> integer * -1 * -2 * -3 * -4)
                .collect(Collectors.toSet());

        final AwaitResultConsumer<Integer> consumer = new AwaitResultConsumer<>(streamSize);
        flame.attachRear("linerFilterRear", new AkkaRearType<>(runtime.system(), Integer.class))
                .forEach(f -> f.addListener(consumer));
        applyDataToAllHandlesAsync(source, handles);

        consumer.await(5, TimeUnit.MINUTES);
        Assert.assertEquals(
                consumer.result().collect(Collectors.toSet()),
                expected
        );
      }
    }
  }

  static final class HumbleFiler implements Function<Integer, Stream<Integer>> {
    private final int factor;

    HumbleFiler(int factor) {
      this.factor = factor;
    }

    @Override
    public Stream<Integer> apply(Integer s) {
      return Stream.of(s * factor);
    }
  }
}
