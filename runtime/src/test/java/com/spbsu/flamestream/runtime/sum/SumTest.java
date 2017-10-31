package com.spbsu.flamestream.runtime.sum;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.barrier.BarrierSuite;
import com.spbsu.flamestream.core.graph.ops.Broadcast;
import com.spbsu.flamestream.core.graph.ops.Filter;
import com.spbsu.flamestream.core.graph.ops.Grouping;
import com.spbsu.flamestream.core.graph.ops.Merge;
import com.spbsu.flamestream.core.graph.ops.StatelessMap;
import com.spbsu.flamestream.runtime.TestEnvironment;
import com.spbsu.flamestream.runtime.TheGraph;
import com.spbsu.flamestream.runtime.environment.local.LocalClusterEnvironment;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class SumTest {

  @SuppressWarnings("Convert2Lambda")
  private static TheGraph sumGraph(Collection<Integer> fronts, AtomicGraph sink) {
    final HashFunction<Numb> identity = HashFunction.constantHash(1);
    final HashFunction<List<Numb>> groupIdentity = HashFunction.constantHash(1);
    //noinspection Convert2Lambda
    @SuppressWarnings("Convert2Lambda") final BiPredicate<Numb, Numb> predicate = new BiPredicate<Numb, Numb>() {
      @Override
      public boolean test(Numb o, Numb o2) {
        return true;
      }
    };

    final Merge merge = new Merge(Arrays.asList(identity, identity));
    final Grouping<Numb> grouping = new Grouping<>(identity, predicate, 2);
    final StatelessMap<List<Numb>, List<Numb>> enricher = new StatelessMap<>(new IdentityEnricher(), groupIdentity);
    final Filter<List<Numb>> junkFilter = new Filter<>(new WrongOrderingFilter(), groupIdentity);
    final StatelessMap<List<Numb>, Sum> reducer = new StatelessMap<>(new Reduce(), groupIdentity);
    final Broadcast<Sum> broadcast = new Broadcast<>(identity, 2);

    final BarrierSuite<Sum> barrier = new BarrierSuite<Sum>(sink);

    final Graph graph = merge.fuse(grouping, merge.outPort(), grouping.inPort())
            .fuse(enricher, grouping.outPort(), enricher.inPort())
            .fuse(junkFilter, enricher.outPort(), junkFilter.inPort())
            .fuse(reducer, junkFilter.outPort(), reducer.inPort())
            .fuse(broadcast, reducer.outPort(), broadcast.inPort())
            .fuse(barrier, broadcast.outPorts().get(0), barrier.inPort())
            .wire(broadcast.outPorts().get(1), merge.inPorts().get(1));

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> merge.inPorts().get(0)));
    return new TheGraph(graph, frontBindings);
  }

  @Test
  public void testSingleFront() throws Exception {
    test(20, 1000, 1);
  }

  @Test
  public void testMultipleFronts() throws Exception {
    test(30, 1000, 4);
  }

  @Test
  public void shortRepeatedTests() throws Exception {
    for (int i = 0; i < 10; ++i) {
      test(5, 10, 4);
    }
  }

  private void test(int tickLength, int inputSize, int fronts) throws Exception {
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(4);
            TestEnvironment environment = new TestEnvironment(lce)) {

      final Deque<Sum> result = new ArrayDeque<>();

      environment.deploy(SumTest.sumGraph(
              environment.availableFronts(),
              environment.wrapInSink(HashFunction.constantHash(1), k -> result.add((Sum) k))
      ), tickLength, 1);

      final List<LongNumb> source = new Random().ints(inputSize)
              .map(i -> i % 100)
              .map(Math::abs)
              .mapToObj(LongNumb::new)
              .collect(Collectors.toList());
      final Consumer<Object> sink = environment.randomFrontConsumer(fronts);
      source.forEach(longNumb -> {
        sink.accept(longNumb);
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      environment.awaitTick(tickLength + 5);

      final long expected = source.stream()
              .reduce(new LongNumb(0L), (a, b) -> new LongNumb(a.value() + b.value()))
              .value();
      final long actual = result.stream().mapToLong(Sum::value).max().orElseThrow(NoSuchElementException::new);

      Assert.assertEquals(actual, expected);
    }
  }
}
