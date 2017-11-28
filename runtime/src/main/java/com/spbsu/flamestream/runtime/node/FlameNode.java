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
import com.spbsu.flamestream.runtime.node.edge.EdgeManager;
import com.spbsu.flamestream.runtime.node.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.node.edge.api.RearInstance;
import com.spbsu.flamestream.runtime.node.graph.LogicGraphManager;
import com.spbsu.flamestream.runtime.node.graph.acker.AttachRegistry;
import com.spbsu.flamestream.runtime.node.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FlameNode extends LoggingActor {
  private final String id;
  private final AttachRegistry registry;

  private final ActorRef edgeManager;
  private final ActorRef negotiator;
  private final ActorRef barrier;

  private ClusterConfig currentConfig;

  private FlameNode(String id, AttachRegistry attachRegistry, ClusterConfig initialConfig) {
    this.id = id;

    this.registry = attachRegistry;
    this.currentConfig = initialConfig;
    this.negotiator = context().actorOf(Negotiator.props(), "negotiator");
    this.barrier = context().actorOf(Barrier.props(), "barrier");
    this.edgeManager = context().actorOf(EdgeManager.props(negotiator), "edge");
  }

  public static Props props(String id, AttachRegistry attachRegistry, ClusterConfig initialConfig) {
    return Props.create(FlameNode.class, id, id, attachRegistry, initialConfig);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(
                    ClusterConfig.class,
                    config -> log().warning("On flight configuration changes are not supported yet")
            )
            .match(Graph.class, graph -> context().actorOf(LogicGraphManager.props(graph, )))
            .build();
  }

  private IntRange localRange() {
    return currentConfig.nodeConfigs().get(id).range().asRange();
  }

  private Map<IntRange, ActorPath> paths() {
    final Map<IntRange, ActorPath>
    return currentConfig.nodeConfigs().collect(Collectors.toMap(
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

  public interface ZookeeperClient {
    AttachRegistry registry();

    void setConfigurationObserver(Consumer<ClusterConfig> observer);

    void setGraphObserver(Consumer<Graph> observer);

    void setFrontObserver(Consumer<FrontInstance<?>> observer);

    void setRearObserver(Consumer<RearInstance<?>> observer);
  }
}
