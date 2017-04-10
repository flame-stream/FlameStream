package com.spbsu.datastream.core.test;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.graph.*;
import com.spbsu.datastream.core.graph.ops.ConsumerSink;
import com.spbsu.datastream.core.graph.ops.SpliteratorSource;
import com.spbsu.datastream.core.node.MyPaths;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

import static com.spbsu.datastream.core.range.RangeConciergeApi.DeployForTick;

public class DeployGraph {
  public static void main(final String... args) throws Exception {
    new DeployGraph().run();
  }

  private void run() throws UnknownHostException {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 12341)
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLoopbackAddress().getHostName()))
            .withFallback(ConfigFactory.load("remote"));
    final ActorSystem system = ActorSystem.create("requester", config);


    final TheGraph theGraph = theGraph();
    final long tick = 1;
    final DeployForTick request = new DeployForTick(theGraph, tick);

    final ActorSelection worker1 = rangeConcierge(system, 7001, new HashRange(Integer.MIN_VALUE, 0));
    final ActorSelection worker2 = rangeConcierge(system, 7002, new HashRange(0, Integer.MAX_VALUE));
    worker1.tell(request, null);
    worker2.tell(request, null);
  }

  private ActorSelection rangeConcierge(final ActorSystem system,
                                        final int port,
                                        final HashRange range) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath rangeConcierge = MyPaths.rangeConcierge(address, range);
    return system.actorSelection(rangeConcierge);
  }

  private TheGraph theGraph() {
    final Spliterator<Integer> spliterator = new IntSpliterator();
    final Source<Integer> source = new SpliteratorSource<>(spliterator, HashFunction.OBJECT_HASH);
    final MarkingFilter filter = new MarkingFilter(HashFunction.OBJECT_HASH);
    final Sink<Integer> sink = new ConsumerSink<>(new PrintlnConsumer(), HashFunction.OBJECT_HASH, HashFunction.OBJECT_HASH);

    final Graph gr = source.fuse(filter, source.outPort(), filter.inPort()).fuse(sink, filter.outPort(), sink.inPort());
    final FlatGraph graph = FlatGraph.flattened(gr);
    return new TheGraph(graph);
  }

  private static class PrintlnConsumer implements Consumer<Integer> {
    @Override
    public void accept(final Integer integer) {
      System.out.println(integer);
    }
  }

  private static class IntSpliterator extends Spliterators.AbstractSpliterator<Integer> {
    private boolean plus = false;

    public IntSpliterator() {
      super(Long.MAX_VALUE, 0);
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Integer> action) {
      if (plus) {
        plus = false;
        action.accept(100);
      } else {
        plus = true;
        action.accept(-100);
      }
      return true;
    }
  }
}
