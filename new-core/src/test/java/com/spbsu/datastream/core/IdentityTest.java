package com.spbsu.datastream.core;

import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.barrier.ConsumerBarrierSink;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.graph.ops.StatelessFilter;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class IdentityTest extends DataStreamsSuite {

  @Test
  public void emptyTest() throws InterruptedException {
    final StatelessFilter<String, String> filter = new StatelessFilter<>(new HumbleFiler(), HashFunction.OBJECT_HASH);
    final PreSinkMetaFilter<String> metaFilter = new PreSinkMetaFilter<>(HashFunction.OBJECT_HASH);
    final ConsumerBarrierSink<String> println = new ConsumerBarrierSink<>(new PrintlnConsumer());

    final Graph graph = filter.fuse(metaFilter, filter.outPort(), metaFilter.inPort())
            .fuse(println, metaFilter.outPort(), println.inPort());

    final Set<Integer> fronts = this.fronts();
    final Map<Integer, InPort> frontBindings = fronts.stream().collect(Collectors.toMap(Function.identity(), e -> filter.inPort()));
    final TheGraph theGraph = new TheGraph(graph, frontBindings);

    this.deploy(theGraph);
    TimeUnit.SECONDS.sleep(2);

    final Consumer<Object> consumer = this.randomConsumer();
    for (int i = 0; i < 10000; ++i) {
      consumer.accept("Val = " + i);
    }

    TimeUnit.MINUTES.sleep(10);
  }

  public static final class PrintlnConsumer implements Consumer<String> {
    @Override
    public void accept(final String s) {
      System.out.println(s);
    }
  }

  public static final class HumbleFiler implements Function<String, String> {

    @Override
    public String apply(final String s) {
      return s + "; Filter was here";
    }
  }
}
