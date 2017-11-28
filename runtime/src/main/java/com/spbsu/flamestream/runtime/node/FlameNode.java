package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.node.config.ClusterConfig;
import com.spbsu.flamestream.runtime.node.graph.GraphManager;
import com.spbsu.flamestream.runtime.node.graph.edge.EdgeManager;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;
import org.apache.zookeeper.ZooKeeper;

import java.util.Map;
import java.util.stream.Collectors;

public class FlameNode extends LoggingActor {
  private final String id;
  private final GraphClient graphClient;
  private final ConfigurationClient configurationClient;
  private final ClusterConfig config;

  private final ActorRef edgeManager;
  private final ActorRef graphManager;

  private FlameNode(String id, ZooKeeper zk) {
    this.id = id;
    this.graphClient = new ZooKeeperFlameClient(zk);
    this.configurationClient = new ZooKeeperFlameClient(zk);

    // TODO: 11/27/17 handle configuration changes
    this.config = configurationClient.configuration(configuration -> {});

    this.edgeManager = context().actorOf(EdgeManager.props(), "edge");
    this.graphManager = context().actorOf(GraphManager.props(systems()), "graph");
  }

  public static Props props(String id, ZooKeeper zooKeeper) {
    return Props.create(FlameNode.class, id, zooKeeper);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match()
            .build();
  }

  private Map<IntRange, ActorPath> paths() {
    return config.nodeConfigs().collect(Collectors.toMap(
            node -> node.range().asRange(),
            node -> {
              final Address system = Address.apply(
                      "akka.tcp",
                      "worker",
                      node.address().host(),
                      node.address().port()
              );
              return RootActorPath.apply(system, "watcher").child("node");
            }
    ));
  }
}
