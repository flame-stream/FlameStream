package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.barrier.BarrierSink;
import com.spbsu.flamestream.core.graph.barrier.PreBarrierMetaFilter;
import com.spbsu.flamestream.core.graph.ops.StatelessMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class FilterAcceptanceTest {

  private static TheGraph multiGraph(Collection<Integer> fronts, AtomicGraph sink) {
    final StatelessMap<Integer, Integer> filter1 = new StatelessMap<>(new HumbleFiler(-1), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter2 = new StatelessMap<>(new HumbleFiler(-2), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter3 = new StatelessMap<>(new HumbleFiler(-3), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter4 = new StatelessMap<>(new HumbleFiler(-4), HashFunction.OBJECT_HASH);

    final PreBarrierMetaFilter<Integer> metaFilter = new PreBarrierMetaFilter<>(HashFunction.OBJECT_HASH);
    final BarrierSink barrierSink = new BarrierSink(sink);

    final Graph graph = filter1.fuse(filter2, filter1.outPort(), filter2.inPort())
            .fuse(filter3, filter2.outPort(), filter3.inPort())
            .fuse(filter4, filter3.outPort(), filter4.inPort())
            .fuse(metaFilter, filter4.outPort(), metaFilter.inPort())
            .fuse(barrierSink, metaFilter.outPort(), barrierSink.inPort());

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> filter1.inPort()));
    return new TheGraph(graph, frontBindings);
  }

  @Test
  public void linearFilter() throws Exception {
    try (TestStand stage = new TestStand(4)) {

      final Queue<Integer> result = new ArrayDeque<>();

      stage.deploy(FilterAcceptanceTest.multiGraph(
              stage.environment().availableFronts(),
              stage.environment().wrapInSink(result::add)
      ), 10, 1);

      final List<Integer> source = new Random().ints(1000).boxed().collect(Collectors.toList());
      final Consumer<Object> sink = stage.randomFrontConsumer(4);
      source.forEach(sink);

      stage.awaitTick(10);

      Assert.assertEquals(new HashSet<>(result), source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet()));
    }
  }

  @Test
  public void multipleTicksLinearFilter() throws Exception {
    try (TestStand stage = new TestStand(4)) {

      final Queue<Integer> result = new ArrayDeque<>();

      stage.deploy(FilterAcceptanceTest.multiGraph(
              stage.environment().availableFronts(),
              stage.environment().wrapInSink(result::add)
      ), 2, 10);

      final List<Integer> source = new Random().ints(20000).boxed().collect(Collectors.toList());
      final Consumer<Object> sink = stage.randomFrontConsumer(4);
      source.forEach(sink);
      stage.awaitTick(40);

      Assert.assertEquals(new HashSet<>(result), source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet()));
    }
  }

  public static final class HumbleFiler implements Function<Integer, Integer> {
    private final int factor;

    public HumbleFiler(int factor) {
      this.factor = factor;
    }

    @Override
    public Integer apply(Integer s) {
      return s * factor;
    }
  }
}
