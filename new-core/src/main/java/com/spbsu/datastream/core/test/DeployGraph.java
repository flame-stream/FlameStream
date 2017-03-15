package com.spbsu.datastream.core.test;

import akka.actor.*;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.graph.FlatGraph;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.Broadcast;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static com.spbsu.datastream.core.deploy.DeployApi.DeployForTick;

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
    final long tick = (int) (System.currentTimeMillis() / TimeUnit.MINUTES.toMillis(13));
    final DeployForTick request = new DeployForTick(theGraph, tick);

    final ActorSelection worker1 = remoteDispatcher(system, 7001, new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE));
    worker1.tell(request, null);
  }

  private ActorSelection remoteDispatcher(final ActorSystem system,
                                          final int port,
                                          final HashRange range) {
    final Address address = Address.apply("akka.tcp", "worker",
            "localhost",
            port);
    final ActorPath dispatcher = RootActorPath.apply(address, "/").$div("user").$div("root").$div(range.toString());
    return system.actorSelection(dispatcher);
  }

  private TheGraph theGraph() {
    //final Spliterator<HashableString> spliterator = Stream.generate(UUID::randomUUID).map(UUID::toString)
    //        .map(HashableString::new)
    //        .limit(100).spliterator();
    //final Source<HashableString> source = new SpliteratorSource<>(spliterator);
    //
    //final Sink<HashableString> sink = new ConsumerSink<>(System.out::println);
    //
    //final Graph gr = source.fuse(sink, source.outPort(), sink.inPort());

    final FlatGraph graph = FlatGraph.flattened(new Broadcast<>(1));
    return new TheGraph(graph);
  }
}
