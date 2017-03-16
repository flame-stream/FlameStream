package com.spbsu.datastream.core.application;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.node.LifecycleWatcher;
import com.spbsu.datastream.core.node.NodeConcierge;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class WorkerApplication {
  public static void main(final String... args) throws IOException {
    int port;
    if (args.length == 0) {
      port = 7001;
    } else {
      port = Integer.valueOf(args[0]);
    }
    new WorkerApplication().run(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
  }

  public void run(final InetSocketAddress address) throws IOException {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + address.getPort())
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + address.getHostString()))
            .withFallback(ConfigFactory.load("remote"));
    final ActorSystem system = ActorSystem.create("worker", config);
    final ActorRef watcher = system.actorOf(LifecycleWatcher.props(), "watcher");

    final ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 5000,
            event -> watcher.tell(event, null));

    final ActorRef concierge = system.actorOf(NodeConcierge.props(address, zooKeeper), "root");
  }
}
