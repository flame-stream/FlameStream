package com.spbsu.flamestream.runtime.sum;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SumTest extends FlameAkkaSuite {

  private static Graph sumGraph() {
    final HashFunction identity = HashFunction.constantHash(1);

    //noinspection Convert2Lambda
    final Equalz predicate = new Equalz() {
      @Override
      public boolean test(DataItem dataItem, DataItem dataItem2) {
        return true;
      }
    };

    final Source source = new Source();
    final Grouping<Numb> grouping = new Grouping<>(identity, predicate, 2, Numb.class);
    final FlameMap<List<Numb>, List<Numb>> enricher = new FlameMap<>(
            new IdentityEnricher(),
            List.class
    );
    final FlameMap<List<Numb>, List<Numb>> junkFilter = new FlameMap<>(
            new WrongOrderingFilter(),
            List.class
    );
    final FlameMap<List<Numb>, Sum> reducer = new FlameMap<>(new Reduce(), List.class);
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
    final int parallelism = 4;
    try (final LocalRuntime runtime = new LocalRuntime(parallelism, 50)) {
      final FlameRuntime.Flame flame = runtime.run(sumGraph());
      {
        final List<AkkaFrontType.Handle<LongNumb>> handles = flame.attachFront("sumFront",
                new AkkaFrontType<LongNumb>(runtime.system(), true)).collect(Collectors.toList());
        final AtomicLong expected = new AtomicLong();
        final int streamSize = 1000;
        final List<Stream<LongNumb>> source = Stream.generate(() -> new Random()
                .ints(streamSize, 0, 100)
                .peek(expected::addAndGet)
                .mapToObj(LongNumb::new))
                .limit(parallelism)
                .collect(Collectors.toList());

        final AwaitConsumer<Sum> consumer = new AwaitConsumer<>(streamSize * parallelism);
        flame.attachRear("sumRear", new AkkaRearType<>(runtime.system(), Sum.class))
                .forEach(r -> r.addListener(consumer));
        applyDataToHandles(source, handles);

        consumer.await(10, TimeUnit.MINUTES);
        final long actual = consumer.result()
                .mapToLong(Sum::value)
                .max()
                .orElseThrow(NoSuchElementException::new);
        Assert.assertEquals(actual, expected.get());
        Assert.assertEquals(consumer.result().count(), streamSize * parallelism);
      }
    }
  }

  @Test(invocationCount = 10)
  public void totalOrderTest() throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime(4)) {
      final FlameRuntime.Flame flame = runtime.run(sumGraph());
      {
        final List<LongNumb> source = new Random()
                .ints(1000)
                .mapToObj(LongNumb::new)
                .collect(Collectors.toList());
        final Set<Sum> expected = new HashSet<>();
        long currentSum = 0;
        for (LongNumb longNumb : source) {
          currentSum += longNumb.value();
          expected.add(new Sum(currentSum));
        }

        final List<AkkaFrontType.Handle<LongNumb>> handles = flame.attachFront(
                "totalOrderFront",
                new AkkaFrontType<LongNumb>(runtime.system(), false)
        ).collect(Collectors.toList());
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).eos();
        }
        final AkkaFrontType.Handle<LongNumb> sink = handles.get(0);

        final AwaitConsumer<Sum> consumer = new AwaitConsumer<>(source.size());
        flame.attachRear("totalOrderRear", new AkkaRearType<>(runtime.system(), Sum.class))
                .forEach(r -> r.addListener(consumer));
        source.forEach(sink);
        sink.eos();

        consumer.await(10, TimeUnit.MINUTES);
        Assert.assertEquals(consumer.result().collect(Collectors.toSet()), expected);
      }
    }
  }
}
