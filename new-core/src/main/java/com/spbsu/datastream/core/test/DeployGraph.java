package com.spbsu.datastream.core.test;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.front.RawData;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.ConsumerBarrierSink;
import com.spbsu.datastream.core.graph.ops.PreSinkMetaFilter;
import com.spbsu.datastream.core.graph.ops.StatelessFilter;
import com.spbsu.datastream.core.node.MyPaths;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.spbsu.datastream.core.range.RangeConciergeApi.DeployForTick;

public final class DeployGraph {
  public static void main(final String... args) {
    new DeployGraph().run();
  }

  private void run() {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 12341)
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLoopbackAddress().getHostName()))
            .withFallback(ConfigFactory.load("remote"));
    final ActorSystem system = ActorSystem.create("requester", config);

    final TheGraph theGraph = DeployGraph.theGraph();
    final long tick = System.currentTimeMillis() / TimeUnit.HOURS.toMillis(1);

    final DeployForTick request = new DeployForTick(theGraph,
            new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE / 2),
            tick,
            System.currentTimeMillis(),
            20);

    final ActorSelection worker1 = DeployGraph.rangeConcierge(system, 7001, new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE / 2));
    final ActorSelection worker2 = DeployGraph.rangeConcierge(system, 7002, new HashRange(Integer.MAX_VALUE / 2, Integer.MAX_VALUE));
    worker1.tell(request, null);
    worker2.tell(request, null);

    final ActorSelection front1 = DeployGraph.front(system, 7001);
    final ActorSelection front2 = DeployGraph.front(system, 7002);
    front1.tell(request, ActorRef.noSender());
    front2.tell(request, ActorRef.noSender());

    final Random rd = new Random();
    try (final BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
      br.lines().forEach(l -> {
        if (rd.nextBoolean()) {
          front1.tell(new RawData<>(l), ActorRef.noSender());
        } else {
          front2.tell(new RawData<>(l), ActorRef.noSender());
        }
      });
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection rangeConcierge(final ActorSystem system,
                                               final int port,
                                               final HashRange range) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath rangeConcierge = MyPaths.rangeConcierge(address, range);
    return system.actorSelection(rangeConcierge);
  }

  private static ActorSelection front(final ActorSystem system,
                                      final int port) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath front = MyPaths.front(address);
    return system.actorSelection(front);
  }

  private static TheGraph theGraph() {
    final StatelessFilter<String, String> filter = new StatelessFilter<>(new MyFunc(), HashFunction.OBJECT_HASH);
    final PreSinkMetaFilter<?> preSinkMetaFilter = new PreSinkMetaFilter<>(HashFunction.OBJECT_HASH);
    final ConsumerBarrierSink<String> sink = new ConsumerBarrierSink<>(new PrintlnConsumer());

    final Graph logicGraph = filter
            .fuse(preSinkMetaFilter, filter.outPort(), preSinkMetaFilter.inPort())
            .fuse(sink, preSinkMetaFilter.outPort(), sink.inPort());

    final Map<Integer, InPort> frontBindings = new HashMap<>();

    frontBindings.put(1, filter.inPort());
    frontBindings.put(2, filter.inPort());

    return new TheGraph(logicGraph, frontBindings);
  }

  public static final class PrintlnConsumer implements Consumer<String> {
    @Override
    public void accept(final String value) {
      System.out.println(value);
    }
  }

  private static final class MyFunc implements Function<String, String> {

    @Override
    public String apply(final String integer) {
      return integer + "; filter was here!";
    }
  }
}
