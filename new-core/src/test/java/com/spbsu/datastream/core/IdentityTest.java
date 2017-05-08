package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorConsumer;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.StatelessFilter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class IdentityTest {

  @Test
  public void emptyTest() throws InterruptedException {
    try (TestStand stage = new TestStand(10)) {

      final Queue<Integer> result = new ArrayDeque<>();

      stage.deploy(IdentityTest.graph(stage.fronts(), stage.wrap(result)));

      final List<Integer> source = new Random().ints(5000).boxed().collect(Collectors.toList());
      source.forEach(stage.randomFrontConsumer());

      stage.waitTick();

      Assert.assertEquals(new HashSet<>(result), source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet()));
    }
  }

  private static TheGraph graph(final Collection<Integer> fronts, final ActorPath consumer) {
    final StatelessFilter<Integer, Integer> filter1 = new StatelessFilter<>(new HumbleFiler(-1), HashFunction.OBJECT_HASH);
    final StatelessFilter<Integer, Integer> filter2 = new StatelessFilter<>(new HumbleFiler(-2), HashFunction.OBJECT_HASH);
    final StatelessFilter<Integer, Integer> filter3 = new StatelessFilter<>(new HumbleFiler(-3), HashFunction.OBJECT_HASH);
    final StatelessFilter<Integer, Integer> filter4 = new StatelessFilter<>(new HumbleFiler(-4), HashFunction.OBJECT_HASH);
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

    public HumbleFiler(final int factor) {
      this.factor = factor;
    }

    @Override
    public Integer apply(final Integer s) {
      return s * this.factor;
    }
  }
}
