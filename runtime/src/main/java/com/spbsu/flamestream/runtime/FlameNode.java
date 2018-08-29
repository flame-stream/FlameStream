package com.spbsu.flamestream.runtime;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.master.acker.Acker;
import com.spbsu.flamestream.runtime.master.acker.Registry;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.EdgeManager;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.graph.GraphManager;
import com.spbsu.flamestream.runtime.edge.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.Map;

public class FlameNode extends LoggingActor {
  private final ActorRef edgeManager;
  private final ClusterConfig config;

  private FlameNode(String id, Graph bootstrapGraph, ClusterConfig config, Registry registry, StateStorage storage) {
    this.config = config;
    final ActorRef acker;
    if (id.equals(config.masterLocation())) {
      acker = context().actorOf(Acker.props(
              config.paths().size(),
              config.defaultMinTime(),
              config.millisBetweenCommits(),
              registry
      ), "acker");
    } else {
      acker = AwaitResolver.syncResolve(config.paths().get(config.masterLocation()).child("acker"), context());
    }

    final ActorRef graph = context().actorOf(GraphManager.props(
            id,
            bootstrapGraph,
            acker,
            config.props(),
            storage
    ), "graph");
    graph.tell(resolvedManagers(), self());

    final ActorRef negotiator = context().actorOf(Negotiator.props(acker, graph), "negotiator");
    this.edgeManager = context().actorOf(EdgeManager.props(config.paths().get(id), id, negotiator, graph), "edge");
  }

  @Override
  public void preStart() {
    log().info("Starting FlameNode with config '{}'", config);
  }

  public static Props props(String id,
                            Graph initialGraph,
                            ClusterConfig initialConfig,
                            Registry registry,
                            StateStorage stateStorage) {
    return Props.create(FlameNode.class, id, initialGraph, initialConfig, registry, stateStorage);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AttachFront.class, f -> edgeManager.forward(f, context()))
            .match(AttachRear.class, f -> edgeManager.forward(f, context()))
            .build();
  }

  private Map<String, ActorRef> resolvedManagers() {
    final Map<String, ActorRef> managers = new HashMap<>();
    config.paths().forEach((id, path) -> {
      final ActorRef manager = AwaitResolver.syncResolve(path.child("graph"), context());
      managers.put(id, manager);
    });
    return managers;
  }
}
