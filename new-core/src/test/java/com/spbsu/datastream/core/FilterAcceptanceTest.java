package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorConsumer;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.StatelessMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class FilterAcceptanceTest {

  @Test
  public void linearFilter() throws InterruptedException {
    try (TestStand stage = new TestStand(4, 4)) {

      final Queue<Integer> result = new ArrayDeque<>();

      stage.deploy(FilterAcceptanceTest.multiGraph(stage.fronts(), stage.wrap(result::add)), 5, TimeUnit.SECONDS);

      final List<Integer> source = new Random().ints(1000).boxed().collect(Collectors.toList());
      final Consumer<Object> sink = stage.randomFrontConsumer();
      source.forEach(sink);
      stage.waitTick(6, TimeUnit.SECONDS);

      Assert.assertEquals(new HashSet<>(result), source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet()));
    }
  }

  private static TheGraph multiGraph(Collection<Integer> fronts, ActorPath consumer) {
    final StatelessMap<Integer, Integer> filter1 = new StatelessMap<>(new HumbleFiler(-1), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter2 = new StatelessMap<>(new HumbleFiler(-2), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter3 = new StatelessMap<>(new HumbleFiler(-3), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter4 = new StatelessMap<>(new HumbleFiler(-4), HashFunction.OBJECT_HASH);
    final PreSinkMetaFilter<Integer> metaFilter = new PreSinkMetaFilter<>(HashFunction.OBJECT_HASH);
    final RemoteActorConsumer<Integer> sink = new RemoteActorConsumer<>(consumer);

    final Graph graph = filter1.fuse(filter2, filter1.outPort(), filter2.inPort())
            .fuse(filter3, filter2.outPort(), filter3.inPort())
            .fuse(filter4, filter3.outPort(), filter4.inPort())
            .fuse(metaFilter, filter4.outPort(), metaFilter.inPort())
            .fuse(sink, metaFilter.outPort(), sink.inPort());

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> filter1.inPort()));
    return new TheGraph(graph, frontBindings);
  }

  public static final class HumbleFiler implements Function<Integer, Integer> {
    private final int factor;

    public HumbleFiler(int factor) {
      this.factor = factor;
    }

    @Override
    public Integer apply(Integer s) {
      return s * this.factor;
    }
  }
}
