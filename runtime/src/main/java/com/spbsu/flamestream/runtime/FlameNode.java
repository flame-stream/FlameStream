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
import com.spbsu.flamestream.runtime.graph.LogicGraphManager;
import com.spbsu.flamestream.runtime.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.negitioator.api.AttachSource;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;
import org.jetbrains.annotations.Nullable;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class FlameNode extends LoggingActor {
  private final String id;
  private final ClusterConfig currentConfig;

  private final ActorRef negotiator;
  private final ActorRef barrier;
  private final ActorRef edgeManager;
  private final ActorRef acker;

  private FlameNode(String id, AttachRegistry attachRegistry, ClusterConfig initialConfig) {
    this.id = id;
    this.currentConfig = initialConfig;

    if (id.equals(currentConfig.ackerLocation())) {
      this.acker = context().actorOf(Acker.props(attachRegistry), "acker");
    } else {
      this.acker = resolvedAcker();
    }

    this.negotiator = context().actorOf(Negotiator.props(acker), "negotiator");
    this.barrier = context().actorOf(Barrier.props(), "barrier");
    this.edgeManager = context().actorOf(EdgeManager.props(negotiator), "edge");
  }

  public static Props props(String id, AttachRegistry attachRegistry, ClusterConfig initialConfig) {
    return Props.create(FlameNode.class, id, attachRegistry, initialConfig);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Graph.class, graph -> {
              final ActorRef graphManager = context().actorOf(LogicGraphManager.props(
                      graph,
                      acker,
                      negotiator,
                      barrier
              ), "graph");

              graphManager.tell(resolvedManagers(), self());
              negotiator.tell(new AttachSource(graphManager), self());
              getContext().become(emptyBehavior());
            })
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

  private IntRangeMap<ActorRef> resolvedManagers() {
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

    return new ListIntRangeMap<>(managers);
  }
}
