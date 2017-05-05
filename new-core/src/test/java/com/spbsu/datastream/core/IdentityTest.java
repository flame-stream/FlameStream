package com.spbsu.datastream.core;

import com.spbsu.datastream.core.barrier.ConsumerBarrierSink;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.StatelessFilter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class IdentityTest extends DataStreamsSuite {

  @Test
  public void emptyTest() throws InterruptedException {
    final Queue<Integer> result = new ArrayDeque<>();

    final StatelessFilter<Integer, Integer> filter1 = new StatelessFilter<>(new HumbleFiler(-1), HashFunction.OBJECT_HASH);
    final StatelessFilter<Integer, Integer> filter2 = new StatelessFilter<>(new HumbleFiler(-2), HashFunction.OBJECT_HASH);
    final StatelessFilter<Integer, Integer> filter3 = new StatelessFilter<>(new HumbleFiler(-3), HashFunction.OBJECT_HASH);
    final StatelessFilter<Integer, Integer> filter4 = new StatelessFilter<>(new HumbleFiler(-4), HashFunction.OBJECT_HASH);
    final PreSinkMetaFilter<Integer> metaFilter = new PreSinkMetaFilter<>(HashFunction.OBJECT_HASH);
    final ConsumerBarrierSink<Integer> consumer = new ConsumerBarrierSink<>(this.wrap(result));

    final Graph graph = filter1.fuse(filter2, filter1.outPort(), filter2.inPort())
            .fuse(filter3, filter2.outPort(), filter3.inPort())
            .fuse(filter4, filter3.outPort(), filter4.inPort())
            .fuse(metaFilter, filter4.outPort(), metaFilter.inPort())
            .fuse(consumer, metaFilter.outPort(), consumer.inPort());

    final Set<Integer> fronts = this.fronts();
    final Map<Integer, InPort> frontBindings = fronts.stream().collect(Collectors.toMap(Function.identity(), e -> filter1.inPort()));
    final TheGraph theGraph = new TheGraph(graph, frontBindings);

    this.deploy(theGraph);
    TimeUnit.SECONDS.sleep(2);

    final List<Integer> source = new Random().ints(10000).boxed().collect(Collectors.toList());
    source.forEach(this.randomConsumer());

    TimeUnit.SECONDS.sleep(30);

    Assert.assertEquals(new HashSet<>(result), source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet()));
  }

  public static final class HumbleFiler implements Function<Integer, Integer> {
    private final int factor;

    public HumbleFiler(final int factor) {
      this.factor = factor;
    }

    @Override
    public Integer apply(final Integer s) {
      return s * factor;
    }
  }
}
