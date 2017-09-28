package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.barrier.BarrierSink;
import com.spbsu.flamestream.core.graph.barrier.PreBarrierMetaFilter;
import com.spbsu.flamestream.core.graph.ops.Grouping;
import com.spbsu.flamestream.core.graph.ops.StatelessMap;
import com.spbsu.flamestream.runtime.environment.local.LocalClusterEnvironment;
import org.jooq.lambda.Collectable;
import org.jooq.lambda.Seq;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class GroupingAcceptanceTest {
  private static void doIt(HashFunction<? super Long> groupHash,
                           HashFunction<? super Long> filterHash,
                           BiPredicate<? super Long, ? super Long> equalz) throws Exception {
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(5); TestEnvironment environment = new TestEnvironment(lce)) {
      final Set<List<Long>> result = new HashSet<>();
      final int window = 7;

      //noinspection unchecked
      environment.deploy(GroupingAcceptanceTest.groupGraph(
              environment.availableFronts(),
              environment.wrapInSink(di -> result.add((List<Long>) di)),
              window,
              groupHash,
              equalz,
              filterHash
      ), 10, 1);

      final List<Long> source = new Random().longs(1000).boxed().collect(Collectors.toList());
      final Consumer<Object> sink = environment.randomFrontConsumer(1);
      source.forEach(sink);
      environment.awaitTick(12);

      Assert.assertEquals(new HashSet<>(result), GroupingAcceptanceTest.expected(source, groupHash, window));
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

  private static TheGraph groupGraph(Collection<Integer> fronts,
                                     AtomicGraph sink,
                                     int window,
                                     HashFunction<? super Long> groupHash,
                                     BiPredicate<? super Long, ? super Long> equalz,
                                     HashFunction<? super Long> filterHash) {
    final StatelessMap<Long, Long> filter = new StatelessMap<>(new Id(), filterHash);
    final Grouping<Long> grouping = new Grouping<>(groupHash, equalz, window);

    final PreBarrierMetaFilter<List<Long>> metaFilter = new PreBarrierMetaFilter<>(HashFunction.OBJECT_HASH);
    final BarrierSink barrierSink = new BarrierSink(sink);

    final Graph graph = filter.fuse(grouping, filter.outPort(), grouping.inPort())
            .fuse(metaFilter, grouping.outPort(), metaFilter.inPort())
            .fuse(barrierSink, metaFilter.outPort(), barrierSink.inPort());

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> filter.inPort()));
    return new TheGraph(graph, frontBindings);
  }

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

  private static final class Id implements Function<Long, Long> {
    @Override
    public Long apply(Long value) {
      return value;
    }
  }
}

