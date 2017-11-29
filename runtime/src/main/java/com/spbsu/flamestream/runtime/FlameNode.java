package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.acker.Acker;
import com.spbsu.flamestream.runtime.acker.AttachRegistry;
import com.spbsu.flamestream.runtime.barrier.Barrier;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.EdgeManager;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.edge.api.RearInstance;
import com.spbsu.flamestream.runtime.graph.BarrierRouter;
import com.spbsu.flamestream.runtime.graph.FlameRouter;
import com.spbsu.flamestream.runtime.graph.LogicGraphManager;
import com.spbsu.flamestream.runtime.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class FlameNode extends LoggingActor {
  private final String id;
  private final ClusterConfig currentConfig;

  private final Graph bootstrapGraph;

  private final ActorRef acker;
  private final ActorRef edgeManager;
  private final ActorRef negotiator;
  private final ActorRef graph;
  private final ActorRef barrier;

  private FlameNode(String id, Graph bootstrapGraph, ClusterConfig initialConfig, AttachRegistry attachRegistry) {
    this.id = id;
    this.currentConfig = initialConfig;
    this.bootstrapGraph = bootstrapGraph;
    if (id.equals(currentConfig.ackerLocation())) {
      this.acker = context().actorOf(Acker.props(attachRegistry), "acker");
    } else {
      this.acker = resolvedAcker();
    }
    this.barrier = context().actorOf(Barrier.props(acker), "barrier");
    this.graph = context().actorOf(LogicGraphManager.props(
            bootstrapGraph,
            acker,
            resolvedBarriers()
    ), "graph");
    graph.tell(resolvedRouter(), self());
    this.negotiator = context().actorOf(Negotiator.props(acker, graph), "negotiator");
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

  private ActorRef resolvedAcker() {
    try {
      final ActorPath ackerPath = currentConfig.nodeConfigs()
              .get(currentConfig.ackerLocation())
              .nodePath()
              .child("acker");
      return context().actorSelection(ackerPath)
              .resolveOneCS(Duration.apply(10, TimeUnit.SECONDS))
              .toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private BarrierRouter resolvedBarriers() {
    final Map<String, ActorRef> barriers = new HashMap<>();
    currentConfig.nodeConfigs().forEach((id, nodeConfig) -> {
      try {
        final ActorRef b = context().actorSelection(nodeConfig.nodePath().child("barrier"))
                .resolveOneCS(FiniteDuration.apply(10, TimeUnit.SECONDS))
                .toCompletableFuture()
                .get();
        barriers.put(id, b);
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    return (item, sender) -> barriers.get(item.meta().globalTime().front()).tell(item, sender);
  }

  private FlameRouter resolvedRouter() {
    final Map<IntRange, ActorRef> managers = new HashMap<>();

    currentConfig.pathsByRange().forEach((intRange, path) -> {
      try {
        final ActorRef manager = context().actorSelection(path.child("graph"))
                .resolveOneCS(FiniteDuration.apply(10, TimeUnit.SECONDS))
                .toCompletableFuture()
                .get();
        managers.put(intRange, manager);
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });

    return (item, sender) -> {};
  }
}
