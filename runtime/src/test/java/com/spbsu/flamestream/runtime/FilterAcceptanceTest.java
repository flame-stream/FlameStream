package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.barrier.BarrierSuite;
import com.spbsu.flamestream.core.graph.ops.StatelessMap;
import com.spbsu.flamestream.runtime.environment.local.LocalClusterEnvironment;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class FilterAcceptanceTest {

  private static TheGraph multiGraph(Collection<Integer> fronts, AtomicGraph sink) {
    final StatelessMap<Integer, Integer> filter1 = new StatelessMap<>(new HumbleFiler(-1), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter2 = new StatelessMap<>(new HumbleFiler(-2), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter3 = new StatelessMap<>(new HumbleFiler(-3), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter4 = new StatelessMap<>(new HumbleFiler(-4), HashFunction.OBJECT_HASH);

    final BarrierSuite<Integer> barrier = new BarrierSuite<Integer>(sink);

    final Graph graph = filter1.fuse(filter2, filter1.outPort(), filter2.inPort())
            .fuse(filter3, filter2.outPort(), filter3.inPort())
            .fuse(filter4, filter3.outPort(), filter4.inPort())
            .fuse(barrier, filter4.outPort(), barrier.inPort());

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> filter1.inPort()));
    return new TheGraph(graph, frontBindings);
  }

  @Test
  public void linearFilter() throws Exception {
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(4);
            TestEnvironment environment = new TestEnvironment(lce)) {
      final Queue<Integer> result = new ArrayDeque<>();
      environment.deploy(FilterAcceptanceTest.multiGraph(
              environment.availableFronts(),
              environment.wrapInSink(HashFunction.OBJECT_HASH, result::add)
      ), 15, 1);

      final List<Integer> source = new Random().ints(1000).boxed().collect(Collectors.toList());
      final Consumer<Object> sink = environment.randomFrontConsumer(4);
      source.forEach(sink);

      environment.awaitTick(15);

      Assert.assertEquals(
              new HashSet<>(result),
              source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet())
      );
    }
  }

  @Test(enabled = false)
  public void multipleTicksLinearFilter() throws Exception {
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(4);
            TestEnvironment environment = new TestEnvironment(lce)) {
      final Queue<Integer> result = new ArrayDeque<>();
      environment.deploy(FilterAcceptanceTest.multiGraph(
              environment.availableFronts(),
              environment.wrapInSink(HashFunction.OBJECT_HASH, result::add)
      ), 2, 10);

      final List<Integer> source = new Random().ints(20000).boxed().collect(Collectors.toList());
      final Consumer<Object> sink = environment.randomFrontConsumer(4);
      source.forEach(sink);
      environment.awaitTick(40);

      Assert.assertEquals(
              new HashSet<>(result),
              source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet())
      );
    }
  }

  public static final class HumbleFiler implements Function<Integer, Integer> {
    private final int factor;

    HumbleFiler(int factor) {
      this.factor = factor;
    }

    @Override
    public Integer apply(Integer s) {
      return s * factor;
    }
  }
}
