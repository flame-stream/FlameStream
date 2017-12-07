package com.spbsu.flamestream.runtime.sum;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.front.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.rear.AkkaRearType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class SumTest extends FlameStreamSuite {

  private static Graph sumGraph() {
    final HashFunction identity = HashFunction.constantHash(1);
    final HashFunction groupIdentity = HashFunction.constantHash(1);

    //noinspection Convert2Lambda
    final BiPredicate<DataItem, DataItem> predicate = new BiPredicate<DataItem, DataItem>() {
      @Override
      public boolean test(DataItem dataItem, DataItem dataItem2) {
        return true;
      }
    };

    final Source source = new Source();
    final Grouping<Numb> grouping = new Grouping<>(identity, predicate, 2, Numb.class);
    final FlameMap enricher = new FlameMap(
            new IdentityEnricher(),
            List.class
    );
    final FlameMap junkFilter = new FlameMap(
            new WrongOrderingFilter(),
            List.class
    );
    final FlameMap reducer = new FlameMap(new Reduce(), List.class);
    final Sink sink = new Sink();

    return new Graph.Builder()
            .link(source, grouping)
            .link(grouping, enricher)
            .link(enricher, junkFilter)
            .link(junkFilter, reducer)
            .link(reducer, sink)
            .link(reducer, grouping)
            .build(source, sink);
  }

  @Test(invocationCount = 10)
  public void sumTest() throws InterruptedException {
    final LocalRuntime runtime = new LocalRuntime(4);
    final FlameRuntime.Flame flame = runtime.run(sumGraph());
    {
      final List<Sum> result = Collections.synchronizedList(new ArrayList<>());
      flame.attachRear("sumRear", new AkkaRearType<>(runtime.system(), Sum.class))
              .forEach(r -> r.addListener(result::add));

      final Consumer<LongNumb> sink = randomConsumer(
              flame.attachFront("sumFront", new AkkaFrontType<LongNumb>(runtime.system())).collect(Collectors.toList())
      );
      final List<LongNumb> source = new Random()
              .ints(1000, 0, 100)
              .mapToObj(LongNumb::new)
              .collect(Collectors.toList());
      final long expected = source.stream().map(LongNumb::value).reduce(Long::sum).orElse(0L);

      source.forEach(sink);
      TimeUnit.SECONDS.sleep(5);

      final long actual = result.stream().mapToLong(Sum::value).max().orElseThrow(NoSuchElementException::new);
      Assert.assertEquals(actual, expected);
    }
  }
}
