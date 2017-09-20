package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import com.spbsu.datastream.core.barrier.BarrierSink;
import com.spbsu.datastream.core.barrier.PreBarrierMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorSink;
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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class GroupingAcceptanceTest {
  @SuppressWarnings("Convert2Lambda")
  @Test
  public void noReorderingSingleHash() throws Exception {
    //noinspection Convert2Lambda
    GroupingAcceptanceTest.doIt(HashFunction.constantHash(100), HashFunction.constantHash(100), new BiPredicate<Long, Long>() {
      @Override
      public boolean test(Long aLong, Long aLong2) {
        return true;
      }
    });
  }

  @Test
  public void noReorderingMultipleHash() throws Exception {
    final HashFunction<Long> hash = HashFunction.uniformLimitedHash(100);
    GroupingAcceptanceTest.doIt(hash, HashFunction.constantHash(100), new BiPredicate<Long, Long>() {

      final HashFunction<Long> hashFunction = hash;

      @Override
      public boolean test(Long aLong, Long aLong2) {
        return hashFunction.hash(aLong) == hashFunction.hash(aLong2);
      }
    });
  }

  @SuppressWarnings("Convert2Lambda")
  @Test
  public void reorderingSingleHash() throws Exception {
    //noinspection Convert2Lambda
    GroupingAcceptanceTest.doIt(HashFunction.constantHash(100), HashFunction.uniformLimitedHash(100), new BiPredicate<Long, Long>() {
      @Override
      public boolean test(Long aLong, Long aLong2) {
        return true;
      }
    });
  }

  @Test
  public void reorderingMultipleHash() throws Exception {
    final HashFunction<Long> hash = HashFunction.uniformLimitedHash(100);

    GroupingAcceptanceTest.doIt(hash, HashFunction.uniformLimitedHash(100), new BiPredicate<Long, Long>() {
      private final HashFunction<Long> hashFunction = hash;

      @Override
      public boolean test(Long aLong, Long aLong2) {
        return hashFunction.hash(aLong) == hashFunction.hash(aLong2);
      }
    });
  }

  private static void doIt(HashFunction<? super Long> groupHash,
                           HashFunction<? super Long> filterHash,
                           BiPredicate<? super Long, ? super Long> equalz) throws Exception {
    try (LocalCluster cluster = new LocalCluster(5, 1);
         TestStand stage = new TestStand(cluster)) {
      final Set<List<Long>> result = new HashSet<>();
      final int window = 7;

      stage.deploy(GroupingAcceptanceTest.groupGraph(stage.frontIds(),
              stage.wrap(di -> result.add((List<Long>) di)),
              window,
              groupHash,
              equalz,
              filterHash), 10, TimeUnit.SECONDS);

      final List<Long> source = new Random().longs(1000).boxed().collect(Collectors.toList());
      final Consumer<Object> sink = stage.randomFrontConsumer(123);
      source.forEach(sink);
      stage.waitTick(12, TimeUnit.SECONDS);

      Assert.assertEquals(new HashSet<>(result), GroupingAcceptanceTest.expected(source, groupHash, window));
    }
  }

  @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
  @Test(enabled = false)
  public void infiniteTest() throws Exception {
    try (LocalCluster cluster = new LocalCluster(5, 1);
         TestStand stage = new TestStand(cluster)) {

      //noinspection Convert2Lambda,Anonymous2MethodRef
      stage.deploy(GroupingAcceptanceTest.groupGraph(stage.frontIds(),
              stage.wrap(d -> {
              }),
              3,
              HashFunction.OBJECT_HASH,
              new BiPredicate<Long, Long>() {
                @Override
                public boolean test(Long aLong, Long aLong2) {
                  return aLong.equals(aLong2);
                }
              }, HashFunction.OBJECT_HASH), 15, TimeUnit.HOURS);

      final Consumer<Object> sink = stage.randomFrontConsumer(123);

      new Random().longs().boxed()
              .forEach(Unchecked.consumer(l -> {
                sink.accept(l);
                Thread.sleep(1);
              }));

      stage.waitTick(15, TimeUnit.HOURS);
    }
  }

  private static Set<List<Long>> expected(List<Long> in, HashFunction<? super Long> hash, int window) {
    final Set<List<Long>> mustHave = new HashSet<>();

    final Map<Integer, List<Long>> buckets =
            in.stream().collect(Collectors.groupingBy(hash::hash));

    for (List<Long> bucket : buckets.values()) {
      for (int i = 0; i < Math.min(bucket.size(), window - 1); ++i) {
        mustHave.add(bucket.subList(0, i + 1));
      }

      Seq.seq(bucket).sliding(window)
              .map(Collectable::toList)
              .forEach(mustHave::add);
    }

    return mustHave;
  }

  private static TheGraph groupGraph(Collection<Integer> fronts, ActorPath consumerPath,
                                     int window,
                                     HashFunction<? super Long> groupHash,
                                     BiPredicate<? super Long, ? super Long> equalz,
                                     HashFunction<? super Long> filterHash) {
    final StatelessMap<Long, Long> filter = new StatelessMap<>(new Id(), filterHash);
    final Grouping<Long> grouping = new Grouping<>(groupHash, equalz, window);

    final PreBarrierMetaFilter<List<Long>> metaFilter = new PreBarrierMetaFilter<>(HashFunction.OBJECT_HASH);
    final RemoteActorSink sink = new RemoteActorSink(consumerPath);
    final BarrierSink barrierSink = new BarrierSink(sink);

    final Graph graph = filter.fuse(grouping, filter.outPort(), grouping.inPort())
            .fuse(metaFilter, grouping.outPort(), metaFilter.inPort())
            .fuse(barrierSink, metaFilter.outPort(), barrierSink.inPort());

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

