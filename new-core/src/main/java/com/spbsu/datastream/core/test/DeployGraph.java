package com.spbsu.datastream.core.test;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.front.RawData;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.node.DeployForTick;
import com.spbsu.datastream.core.node.MyPaths;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public final class DeployGraph {
  public static void main(final String... args) {
    new DeployGraph().run();
  }

  private void run() {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 12341)
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLoopbackAddress().getHostName()))
            .withFallback(ConfigFactory.load("remote"));
    final ActorSystem system = ActorSystem.create("requester", config);


    final long tick = System.currentTimeMillis() / TimeUnit.HOURS.toMillis(1);

    //final DeployForTick request = new DeployForTick(theGraph,
    //        new HashRange(Integer.MIN_VALUE, 0),
    //        tick,
    //        System.currentTimeMillis(),
    //        20);
    //
    //final ActorSelection node1 = DeployGraph.nodeConcierge(system, 7001);
    //final ActorSelection node2 = DeployGraph.nodeConcierge(system, 7002);
    //node1.tell(request, ActorRef.noSender());
    //node2.tell(request, ActorRef.noSender());
    //
    //final ActorSelection front1 = DeployGraph.front(system, 7001);
    //final ActorSelection front2 = DeployGraph.front(system, 7002);
    //
    //final Random rd = new Random();
    //try (final BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
    //  br.lines().forEach(l -> {
    //    if (rd.nextBoolean()) {
    //      front1.tell(new RawData<>(l), ActorRef.noSender());
    //    } else {
    //      front2.tell(new RawData<>(l), ActorRef.noSender());
    //    }
    //  });
    //} catch (final IOException e) {
    //  throw new RuntimeException(e);
    //}
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection nodeConcierge(final ActorSystem system,
                                              final int port) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath rangeConcierge = MyPaths.nodeConcierge(address);
    return system.actorSelection(rangeConcierge);
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection front(final ActorSystem system,
                                      final int port) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath front = MyPaths.front(address);
    return system.actorSelection(front);
  }

}
