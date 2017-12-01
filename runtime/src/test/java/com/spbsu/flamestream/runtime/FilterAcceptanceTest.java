package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FilterAcceptanceTest extends FlameStreamSuite {

  private static Graph multiGraph() {
    final Source source = new Source();
    final FlameMap<Integer, Integer> filter1 = new FlameMap<>(new HumbleFiler(-1));
    final FlameMap<Integer, Integer> filter2 = new FlameMap<>(new HumbleFiler(-2));
    final FlameMap<Integer, Integer> filter3 = new FlameMap<>(new HumbleFiler(-3));
    final FlameMap<Integer, Integer> filter4 = new FlameMap<>(new HumbleFiler(-4));
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
    TestRuntime testRuntime = new TestRuntime(4);
    final TestRuntime.TestFlame flame = testRuntime.run(multiGraph());

    final Queue<Integer> result = new ArrayDeque<>();
    flame.attachWrappedConsumer(result::add);

    final List<Integer> source = new Random().ints(1000).boxed().collect(Collectors.toList());
    final Consumer<Object> front = flame.attachBalancingFront();
    source.forEach(front);

    TimeUnit.SECONDS.sleep(20);

    Assert.assertEquals(
            new HashSet<>(result),
            source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet())
    );
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
