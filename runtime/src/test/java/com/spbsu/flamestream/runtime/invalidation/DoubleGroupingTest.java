package com.spbsu.flamestream.runtime.invalidation;

import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DoubleGroupingTest extends FlameStreamSuite {
  private static final int WINDOW = 2;
  private static final HashFunction HASH_FUNCTION = HashFunction.uniformHash(HashFunction.bucketedHash(
          HashFunction.objectHash(Integer.class),
          20
  ));
  private static final Equalz EQUALZ = Equalz.hashEqualz(HASH_FUNCTION);

  @SuppressWarnings("Convert2Lambda")
  private Graph graph() {
    final Grouping<Integer> firstGroup = new Grouping<>(
            HASH_FUNCTION,
            EQUALZ,
            WINDOW,
            Integer.class
    );
    final FlameMap<List<Integer>, Integer> firstHash = new FlameMap<>(new Function<List<Integer>, Stream<Integer>>() {
      @Override
      public Stream<Integer> apply(List<Integer> longs) {
        return Stream.of(longs.hashCode());
      }
    }, List.class);
    final Grouping<Integer> secondGroup = new Grouping<>(
            HASH_FUNCTION,
            EQUALZ,
            WINDOW,
            Integer.class
    );
    final FlameMap<List<Integer>, Integer> secondHash = new FlameMap<>(new Function<List<Integer>, Stream<Integer>>() {
      @Override
      public Stream<Integer> apply(List<Integer> longs) {
        return Stream.of(longs.hashCode());
      }
    }, List.class);
    final Grouping<Integer> thirdGroup = new Grouping<>(
            HASH_FUNCTION,
            EQUALZ,
            WINDOW,
            Integer.class
    );
    final FlameMap<List<Integer>, Integer> thirdHash = new FlameMap<>(new Function<List<Integer>, Stream<Integer>>() {
      @Override
      public Stream<Integer> apply(List<Integer> longs) {
        return Stream.of(longs.hashCode());
      }
    }, List.class);


    final Source source = new Source();
    final Sink sink = new Sink();

    return new Graph.Builder()
            .link(source, firstGroup)
            .link(firstGroup, firstHash)
            .link(firstHash, secondGroup)
            .link(secondGroup, secondHash)
            .link(secondHash, thirdGroup)
            .link(thirdGroup, thirdHash)
            .link(thirdHash, sink)
            .build(source, sink);
  }

  @Test(invocationCount = 10)
  public void singleWorkerTest() throws InterruptedException {
    doubleGroupingTest(1);
  }

  @Test(invocationCount = 10)
  public void multipleWorkersTest() throws InterruptedException {
    doubleGroupingTest(4);
  }

  // TODO: 3/2/18 Return non-backpressure mode
  private void doubleGroupingTest(int nodes) throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime.Builder().parallelism(nodes).maxElementsInGraph(100).build()) {
      final FlameRuntime.Flame flame = runtime.run(graph());
      {
        final List<AkkaFront.FrontHandle<Integer>> handles = flame.attachFront(
                "doubleGroupingFront",
                new AkkaFrontType<Integer>(runtime.system(), false)
        ).collect(Collectors.toList());
        final AkkaFront.FrontHandle<Integer> sink = handles.get(0);
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).unregister();
        }

        final List<Integer> source = new Random()
                .ints(10000)
                .boxed().collect(Collectors.toList());

        final List<Integer> expected = expected(source);
        final AwaitResultConsumer<Integer> consumer = new AwaitResultConsumer<>(expected.size());
        flame.attachRear("doubleGroupingRear", new AkkaRearType<>(runtime.system(), Integer.class))
                .forEach(r -> r.addListener(consumer));
        source.forEach(sink);
        sink.eos();

        consumer.await(10, TimeUnit.MINUTES);
        Assert.assertEquals(consumer.result().collect(Collectors.toSet()), new HashSet<>(expected));
      }
    }
  }

  @Test(invocationCount = 10)
  public void blinkTest() throws Exception {
    final int iterations = 5;
    final int iterationSize = 12345;
    final Random rd = new Random(1);

    final List<List<Integer>> source = Stream.generate(() ->
            rd.ints(iterationSize).boxed().collect(Collectors.toList())
    ).limit(iterations).collect(Collectors.toList());
    final Set<Integer> expected = new HashSet<>(expected(source.stream()
            .flatMap(List::stream)
            .collect(Collectors.toList())));

    try (final LocalRuntime runtime = new LocalRuntime.Builder().parallelism(4).millisBetweenCommits(5).build()) {
      final AkkaFrontType<Integer> front = new AkkaFrontType<>(runtime.system());
      final AkkaRearType<Integer> rear = new AkkaRearType<>(runtime.system(), Integer.class);
      final AwaitResultConsumer<Integer> consumer = new AwaitResultConsumer<>(expected.size(), HashSet::new);
      final Graph graph = graph();

      for (int iter = 0; iter < iterations; ++iter) {
        FlameRuntime.Flame flame = runtime.run(graph);
        final List<AkkaFront.FrontHandle<Integer>> handles = flame.attachFront("blinkFront", front)
                .collect(Collectors.toList());
        final AkkaFront.FrontHandle<Integer> sink = handles.get(0);
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).unregister();
        }

        flame.attachRear("doubleGroupingRear", rear).forEach(r -> r.addListener(consumer));
        source.get(iter).forEach(sink);

        if (iter != (iterations - 1)) {
          //Thread.sleep(10);
          flame.close();
          Thread.sleep(10);
        } else {
          sink.eos();
        }
      }
      consumer.await(10, TimeUnit.MINUTES);
      Assert.assertEquals(consumer.result().collect(Collectors.toSet()), expected);
    }
  }

  private List<Integer> expected(List<Integer> source) {
    return semanticGrouping(
            semanticGrouping(
                    semanticGrouping(
                            source
                    ).stream().map(List::hashCode).collect(Collectors.toList())
            ).stream().map(List::hashCode).collect(Collectors.toList())
    ).stream().map(List::hashCode).collect(Collectors.toList());
  }

  private static <T> List<List<T>> semanticGrouping(List<T> toBeGrouped) {
    final List<List<T>> result = new ArrayList<>();

    final Map<Wrapper<T>, List<T>> groups = new HashMap<>();
    for (T item : toBeGrouped) {
      final List<T> currentGroup = groups.getOrDefault(new Wrapper<>(item), new ArrayList<>());
      currentGroup.add(item);
      result.add(new ArrayList<>(currentGroup.subList(Math.max(0, currentGroup.size() - WINDOW), currentGroup.size())));
      groups.put(new Wrapper<>(item), currentGroup);
    }

    return result;
  }

  private static class Wrapper<T> {
    private final T value;

    private Wrapper(T value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Wrapper<?> wrapper = (Wrapper<?>) o;
      return EQUALZ.test(
              new PayloadDataItem(new Meta(GlobalTime.MAX), value),
              new PayloadDataItem(new Meta(GlobalTime.MAX), wrapper.value)
      );
    }

    @Override
    public int hashCode() {
      return HASH_FUNCTION.hash(new PayloadDataItem(new Meta(GlobalTime.MAX), value));
    }
  }
}
