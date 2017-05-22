package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorConsumer;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.Grouping;
import com.spbsu.datastream.core.graph.ops.StatelessMap;
import org.jooq.lambda.Collectable;
import org.jooq.lambda.Seq;
import org.jooq.lambda.Unchecked;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class GroupingAcceptanceTest {
  @Test
  public void noReorderingSingleHash() throws InterruptedException {
    GroupingAcceptanceTest.doIt(HashFunction.constantHash(100), HashFunction.constantHash(100));
  }

  @Test
  public void noReorderingMultipleHash() throws InterruptedException {
    GroupingAcceptanceTest.doIt(HashFunction.uniformLimitedHash(100), HashFunction.constantHash(100));
  }

  @Test
  public void reorderingSingleHash() throws InterruptedException {
    GroupingAcceptanceTest.doIt(HashFunction.constantHash(100), HashFunction.OBJECT_HASH);
  }

  @Test
  public void reorderingMultipleHash() throws InterruptedException {
    GroupingAcceptanceTest.doIt(HashFunction.uniformLimitedHash(100), HashFunction.OBJECT_HASH);
  }

  private static void doIt(HashFunction<? super Long> groupHash,
                           HashFunction<? super Long> filterHash) throws InterruptedException {
    try (TestStand stage = new TestStand(10, 1)) {

      final Deque<List<Long>> result = new ArrayDeque<>();

      stage.deploy(GroupingAcceptanceTest.groupGraph(stage.fronts(),
              stage.wrap(result::add),
              groupHash,
              filterHash), 15, TimeUnit.SECONDS);

      final List<Long> source = new Random().longs(10000).boxed().collect(Collectors.toList());
      final Consumer<Object> sink = stage.randomFrontConsumer();
      source.forEach(sink);
      stage.waitTick(15, TimeUnit.SECONDS);

      Assert.assertEquals(new HashSet<>(result), GroupingAcceptanceTest.expected(source, groupHash));
    }
  }

  @Test(enabled = false)
  public void infiniteTest() throws InterruptedException {
    try (TestStand stage = new TestStand(10, 10)) {

      stage.deploy(GroupingAcceptanceTest.groupGraph(stage.fronts(),
              stage.wrap(d -> {}),
              HashFunction.uniformLimitedHash(100),
              HashFunction.OBJECT_HASH), 15, TimeUnit.HOURS);

      final Consumer<Object> sink = stage.randomFrontConsumer();

      new Random().longs().boxed()
              .forEach(Unchecked.consumer(l -> {
                sink.accept(l);
                Thread.sleep(1);
              }));

      stage.waitTick(15, TimeUnit.HOURS);
    }
  }

  private static Set<List<Long>> expected(List<Long> in, HashFunction<? super Long> hash) {
    final Set<List<Long>> mustHave = new HashSet<>();

    final Map<Integer, List<Long>> buckets =
            in.stream().collect(Collectors.groupingBy(hash::hash));

    for (List<Long> bucket : buckets.values()) {
      Seq.seq(bucket).sliding(2)
              .map(Collectable::toList)
              .forEach(mustHave::add);
      if (!bucket.isEmpty()) {
        mustHave.add(Collections.singletonList(bucket.get(0)));
      }
    }

    return mustHave;
  }

  private static TheGraph groupGraph(Collection<Integer> fronts, ActorPath consumer,
                                     HashFunction<? super Long> groupHash,
                                     HashFunction<? super Long> filterHash) {
    final StatelessMap<Long, Long> filter = new StatelessMap<>(new Id(), filterHash);
    final Grouping<Long> grouping = new Grouping<>(groupHash, 2);

    final PreSinkMetaFilter<List<Long>> metaFilter = new PreSinkMetaFilter<>(HashFunction.OBJECT_HASH);
    final RemoteActorConsumer<List<Long>> sink = new RemoteActorConsumer<>(consumer);

    final Graph graph = filter.fuse(grouping, filter.outPort(), grouping.inPort())
            .fuse(metaFilter, grouping.outPort(), metaFilter.inPort())
            .fuse(sink, metaFilter.outPort(), sink.inPort());

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> filter.inPort()));
    return new TheGraph(graph, frontBindings);
  }

  private static final class Id implements Function<Long, Long> {
    @Override
    public Long apply(Long value) {
      return value;
    }
  }
}

