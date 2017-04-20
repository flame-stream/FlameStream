package com.spbsu.datastream.core.test;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.feedback.FeedBackCircuit;
import com.spbsu.datastream.core.graph.FlatGraph;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.ConsumerBarrierSink;
import com.spbsu.datastream.core.graph.ops.PreSinkMetaFilter;
import com.spbsu.datastream.core.graph.ops.SpliteratorSource;
import com.spbsu.datastream.core.graph.ops.StatelessFilter;
import com.spbsu.datastream.core.node.MyPaths;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.agrona.collections.ObjectHashSet;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import static com.spbsu.datastream.core.range.RangeConciergeApi.DeployForTick;

public class DeployGraph {
  public static void main(final String... args) throws UnknownHostException {
    new DeployGraph().run();
  }

  private void run() throws UnknownHostException {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 12341)
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLoopbackAddress().getHostName()))
            .withFallback(ConfigFactory.load("remote"));
    final ActorSystem system = ActorSystem.create("requester", config);


    final TheGraph theGraph = DeployGraph.theGraph();
    final long tick = 1;
    final DeployForTick request = new DeployForTick(theGraph, tick);

    final ActorSelection worker1 = DeployGraph.rangeConcierge(system, 7001, new HashRange(Integer.MIN_VALUE, 0));
    final ActorSelection worker2 = DeployGraph.rangeConcierge(system, 7002, new HashRange(0, Integer.MAX_VALUE));
    worker1.tell(request, null);
    worker2.tell(request, null);
  }

  private static ActorSelection rangeConcierge(final ActorSystem system,
                                               final int port,
                                               final HashRange range) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath rangeConcierge = MyPaths.rangeConcierge(address, range);
    return system.actorSelection(rangeConcierge);
  }

  private static TheGraph theGraph() {
    final Spliterator<Integer> spliterator = new IntSpliterator();
    final SpliteratorSource<Integer> source = new SpliteratorSource<>(spliterator);
    final StatelessFilter<Integer, Integer> filter = new StatelessFilter<>(new MyFunc(), HashFunction.OBJECT_HASH);

    final PreSinkMetaFilter<?> preSinkMetaFilter = new PreSinkMetaFilter<>(HashFunction.OBJECT_HASH);
    final ConsumerBarrierSink<Integer> sink = new ConsumerBarrierSink<>(new PrintlnConsumer());

    final Graph gr = source
            .fuse(filter, source.outPort(), filter.inPort())
            .fuse(preSinkMetaFilter, filter.outPort(), preSinkMetaFilter.inPort())
            .fuse(sink, preSinkMetaFilter.outPort(), sink.inPort());

    final FeedBackCircuit feedBackCircuit = new FeedBackCircuit(4, 1);

    final Graph completeGraph = gr.fuse(feedBackCircuit, source.ackPort(), feedBackCircuit.inPorts().get(0))
            .wire(filter.ackPort(), feedBackCircuit.inPorts().get(1))
            .wire(preSinkMetaFilter.ackPort(), feedBackCircuit.inPorts().get(2))
            .wire(sink.ackPort(), feedBackCircuit.inPorts().get(3))
            .wire(feedBackCircuit.outPorts().get(0), sink.feedbackPort());

    final FlatGraph graph = FlatGraph.flattened(completeGraph);
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

  private static final class MyFunc implements Function<Integer, Integer> {

    @Override
    public Integer apply(final Integer integer) {
      return integer + 1;
    }
  }
}
