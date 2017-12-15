package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.edge.front.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.rear.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.util.AwaitConsumer;
import org.jooq.lambda.Collectable;
import org.jooq.lambda.Seq;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressWarnings("Convert2Lambda")
public final class GroupingAcceptanceTest extends FlameStreamSuite {
  @Test
  public void reorderingMultipleHash() throws InterruptedException {
    final int window = 2;
    final LocalRuntime runtime = new LocalRuntime(4);

    final Graph graph = groupGraph(
            window,
            HashFunction.uniformHash(HashFunction.objectHash(Long.class)),
            new Equalz() {
              @Override
              public boolean test(DataItem dataItem, DataItem dataItem2) {
                return dataItem.payload(Long.class).equals(dataItem2.payload(Long.class));
              }
            }
    );

    final FlameRuntime.Flame flame = runtime.run(graph);
    {

      final List<Long> source = new Random().longs(10000, 0, 10)
              .boxed()
              .collect(Collectors.toList());
      final Consumer<Object> front = randomConsumer(
              flame.attachFront("groupingAcceptanceFront", new AkkaFrontType<>(runtime.system()))
                      .collect(Collectors.toList())
      );
      final Set<List<Long>> expected = GroupingAcceptanceTest.expected(source, window);

      final AwaitConsumer<List<Long>> consumer = new AwaitConsumer<>(expected.size());
      flame.attachRear("groupingAcceptanceRear", new AkkaRearType<>(runtime.system(), List.class))
              .forEach(r -> r.addListener(consumer::accept));

      source.forEach(front);
      TimeUnit.SECONDS.sleep(5);

      Assert.assertEquals(consumer.result().collect(Collectors.toSet()), expected);
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

  private static Graph groupGraph(int window, HashFunction groupHash, Equalz equalz) {
    final Source source = new Source();
    final Grouping<Long> grouping = new Grouping<>(groupHash, equalz, window, Long.class);
    final Sink sink = new Sink();

    return new Graph.Builder().link(source, grouping).link(grouping, sink).build(source, sink);
  }
}
