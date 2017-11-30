package com.spbsu.flamestream.runtime;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.acker.Acker;
import com.spbsu.flamestream.runtime.acker.AttachRegistry;
import com.spbsu.flamestream.runtime.barrier.Barrier;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.NodeConfig;
import com.spbsu.flamestream.runtime.edge.EdgeManager;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.edge.api.RearInstance;
import com.spbsu.flamestream.runtime.graph.GraphManager;
import com.spbsu.flamestream.runtime.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class FlameNode extends LoggingActor {
  private final ActorRef edgeManager;

  private FlameNode(String id, Graph bootstrapGraph, ClusterConfig config, AttachRegistry attachRegistry) {
    final ActorRef acker;
    final NodeConfig ackerNode = config.ackerNode();
    if (id.equals(ackerNode.id())) {
      acker = context().actorOf(Acker.props(attachRegistry), "acker");
    } else {
      acker = AwaitResolver.syncResolve(ackerNode.nodePath().child("acker"), context());
    }

    final ActorRef barrier = context().actorOf(Barrier.props(acker), "barrier");
    final ActorRef graph = context().actorOf(GraphManager.props(bootstrapGraph, acker, config), "graph");
    final ActorRef negotiator = context().actorOf(Negotiator.props(acker, graph), "negotiator");
    this.edgeManager = context().actorOf(EdgeManager.props(id, negotiator, barrier), "edge");
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
}
