package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.edge.front.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.rear.AkkaRearType;
import org.jooq.lambda.Collectable;
import org.jooq.lambda.Seq;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class GroupingAcceptanceTest extends FlameStreamSuite {
  @Test
  public void reorderingMultipleHash() throws InterruptedException {
    final int window = 2;
    final LocalRuntime runtime = new LocalRuntime(10);

    final Graph graph = groupGraph(
            window,
            HashFunction.uniformObjectHash(Long.class),
            new BiPredicate<DataItem, DataItem>() {
              @Override
              public boolean test(DataItem dataItem, DataItem dataItem2) {
                return dataItem.payload(Long.class).equals(dataItem2.payload(Long.class));
              }
            }
    );

    final FlameRuntime.Flame flame = runtime.run(graph);
    {
      final Set<List<Long>> result = Collections.synchronizedSet(new HashSet<>());
      flame.attachRear("groupingAcceptanceRear", new AkkaRearType<>(runtime.system(), List.class))
              .forEach(r -> r.addListener(result::add));

      final List<Long> source = new Random().longs(1000, 0, 1)
              .boxed()
              .collect(Collectors.toList());
      final Consumer<Object> front = randomConsumer(
              flame.attachFront("groupingAcceptanceFront", new AkkaFrontType(runtime.system()))
                      .collect(Collectors.toList())
      );
      //final Consumer<Object> front = flame.attachFront("groupingAcceptanceFront", new AkkaFrontType(runtime.system()))
      //                .collect(Collectors.toList()).get(0);
      source.forEach(front);

      TimeUnit.SECONDS.sleep(20);

      Assert.assertEquals(new HashSet<>(result), GroupingAcceptanceTest.expected(source, window));
    }
  }

  private static Set<List<Long>> expected(List<Long> in, int window) {
    final Set<List<Long>> mustHave = new HashSet<>();
    final Map<Integer, List<Long>> buckets = in.stream().collect(Collectors.groupingBy(Object::hashCode));
    for (List<Long> bucket : buckets.values()) {
      for (int i = 0; i < Math.min(bucket.size(), window - 1); ++i) {
        mustHave.add(bucket.subList(0, i + 1));
      }
      Seq.seq(bucket).sliding(window).map(Collectable::toList).forEach(mustHave::add);
    }
    return mustHave;
  }

  private static Graph groupGraph(int window, HashFunction groupHash, BiPredicate<DataItem, DataItem> equalz) {
    final Source source = new Source();
    final Grouping<Long> grouping = new Grouping<>(groupHash, equalz, window, Long.class);
    final Sink sink = new Sink();

    return new Graph.Builder().link(source, grouping).link(grouping, sink).build(source, sink);
  }
}
