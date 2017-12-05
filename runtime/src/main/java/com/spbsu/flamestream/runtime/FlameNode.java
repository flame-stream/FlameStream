package com.spbsu.flamestream.runtime;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.runtime.acker.Acker;
import com.spbsu.flamestream.runtime.acker.AttachRegistry;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.barrier.Barrier;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.EdgeManager;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.edge.api.RearInstance;
import com.spbsu.flamestream.runtime.graph.GraphManager;
import com.spbsu.flamestream.runtime.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class FlameNode extends LoggingActor {
  private final ActorRef edgeManager;
  private final ActorRef acker;
  private final ClusterConfig config;

  private FlameNode(String id, Graph bootstrapGraph, ClusterConfig config, AttachRegistry attachRegistry) {
    this.config = config;
    if (id.equals(config.ackerLocation())) {
      this.acker = context().actorOf(Acker.props(System.currentTimeMillis(), attachRegistry), "acker");
    } else {
      this.acker = AwaitResolver.syncResolve(config.paths().get(config.ackerLocation()).child("acker"), context());
    }

    final ActorRef barrier = context().actorOf(Barrier.props(acker), "barrier");
    final ActorRef graph = context().actorOf(GraphManager.props(
            id,
            bootstrapGraph,
            acker,
            config.layout(),
            resolvedBarriers()
    ), "graph");
    graph.tell(resolvedManagers(), self());

    final ActorRef negotiator = context().actorOf(Negotiator.props(id, acker, graph), "negotiator");
    this.edgeManager = context().actorOf(EdgeManager.props(config.paths().get(id), id, negotiator, barrier), "edge");
  }

  public static Props props(String id, Graph initialGraph, ClusterConfig initialConfig, AttachRegistry attachRegistry) {
    return Props.create(FlameNode.class, id, initialGraph, initialConfig, attachRegistry);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FrontInstance.class, f -> edgeManager.forward(f, context()))
            .match(RearInstance.class, f -> edgeManager.forward(f, context()))
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

  private BiConsumer<DataItem, ActorRef> resolvedBarriers() {
    final List<ActorRef> barriers = new ArrayList<>();
    config.paths().values().forEach(path -> {
      final ActorRef b = AwaitResolver.syncResolve(path.child("barrier"), context());
      barriers.add(b);
    });
    return (item, sender) -> {
      acker.tell(new Ack(item.meta().globalTime(), item.xor()), sender);
      // FIXME: 12/1/17 Possible error prone location
      final int hash = HashFunction.UNIFORM_OBJECT_HASH.applyAsInt(item.meta().globalTime().time());
      barriers.get(Math.abs(hash) % barriers.size()).tell(item, sender);
    };
  }
}
