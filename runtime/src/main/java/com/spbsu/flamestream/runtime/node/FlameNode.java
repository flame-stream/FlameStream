package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.barrier.Barrier;
import com.spbsu.flamestream.runtime.node.config.ClusterConfig;
import com.spbsu.flamestream.runtime.node.config.ConfigurationClient;
import com.spbsu.flamestream.runtime.node.graph.LogicGraphManager;
import com.spbsu.flamestream.runtime.node.edge.EdgeManager;
import com.spbsu.flamestream.runtime.node.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.node.edge.api.RearInstance;
import com.spbsu.flamestream.runtime.node.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;
import org.apache.zookeeper.ZooKeeper;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FlameNode extends LoggingActor {
  private final String id;
  private final NodeNotifier notifier;
  private final ConfigurationClient configurationClient;
  private final ClusterConfig config;

  private final ActorRef edgeManager;
  private final ActorRef graphManager;
  private final ActorRef negotiator;
  private final ActorRef barrier;

  private FlameNode(String id, ConfigurationClient configurationClient, NodeNotifier notifier) {
    this.id = id;
    this.notifier = notifier;
    this.configurationClient = configurationClient;

    this.negotiator = context().actorOf(Negotiator.props(), "negotiator");
    this.barrier = context().actorOf(Barrier.props(), "barrier");

    this.config = configurationClient.configuration(configuration -> {});

    this.graphManager = context().actorOf(LogicGraphManager.props(systems()), "graph");

    this.edgeManager = context().actorOf(EdgeManager.props(), "edge");
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

  public interface NodeNotifier {
    void setGraphObserver(Consumer<Graph> observer);

    void setFrontObserver(Consumer<FrontInstance<?>> observer);

    void setRearObserver(Consumer<RearInstance<?>> rearObserver);
  }
}
