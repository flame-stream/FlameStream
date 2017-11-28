package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.acker.Acker;
import com.spbsu.flamestream.runtime.node.acker.AttachRegistry;
import com.spbsu.flamestream.runtime.node.barrier.Barrier;
import com.spbsu.flamestream.runtime.node.config.ClusterConfig;
import com.spbsu.flamestream.runtime.node.edge.EdgeManager;
import com.spbsu.flamestream.runtime.node.graph.LogicGraphManager;
import com.spbsu.flamestream.runtime.node.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;
import org.jetbrains.annotations.Nullable;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class FlameNode extends LoggingActor {
  private final ClusterConfig currentConfig;

  private final ActorRef negotiator;
  private final ActorRef barrier;

  @Nullable
  private final ActorRef acker;

  @Nullable
  private ActorRef logicGraphManager = null;


  private FlameNode(String id, AttachRegistry attachRegistry, ClusterConfig initialConfig) {
    this.currentConfig = initialConfig;

    if (id.equals(initialConfig.ackerLocation())) {
      this.acker = context().actorOf(Acker.props(attachRegistry), "acker");
    } else {
      this.acker = null;
    }

    this.negotiator = context().actorOf(Negotiator.props(), "negotiator");
    this.barrier = context().actorOf(Barrier.props(), "barrier");
    context().actorOf(EdgeManager.props(negotiator), "edge");
  }

  public static Props props(String id, AttachRegistry attachRegistry, ClusterConfig initialConfig) {
    return Props.create(FlameNode.class, id, id, attachRegistry, initialConfig);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Graph.class, graph -> {
              logicGraphManager = context().actorOf(LogicGraphManager.props(
                      graph,
                      acker,
                      negotiator,
                      barrier
              ), "graph");
              logicGraphManager.tell(resolvedManagers(), self());
            })
            .build();
  }

  private IntRangeMap<ActorRef> resolvedManagers() {
    final Map<IntRange, ActorRef> managers = new HashMap<>();

    currentConfig.pathsByRange().forEach((intRange, path) -> {
      try {
        final ActorRef manager = context().actorSelection(path)
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
