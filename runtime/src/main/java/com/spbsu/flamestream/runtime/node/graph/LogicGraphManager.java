package com.spbsu.flamestream.runtime.node.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.graph.acker.Acker;
import com.spbsu.flamestream.runtime.node.graph.api.FlameRoutes;
import com.spbsu.flamestream.runtime.node.graph.api.RangeMaterialization;
import com.spbsu.flamestream.runtime.node.graph.materialization.GraphMaterialization;
import com.spbsu.flamestream.runtime.node.negitioator.api.NewMaterialization;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class LogicGraphManager extends LoggingActor {
  private final Graph logicalGraph;

  private final IntRange localRange;
  private final Map<IntRange, ActorRef> managers;
  private final ActorRef materialization;
  private final ActorRef negotiator;
  private final ActorRef barrier;
  private final IntRange ackerRange;

  @Nullable
  private final ActorRef localAcker;

  private LogicGraphManager(Graph logicalGraph,
                            IntRange localRange,
                            IntRange ackerRange,
                            Map<IntRange, ActorRef> managers,
                            ActorRef negotiator,
                            ActorRef barrier) {
    this.localRange = localRange;
    this.logicalGraph = logicalGraph;
    this.ackerRange = ackerRange;
    this.negotiator = negotiator;
    this.barrier = barrier;
    this.managers = managers;
    this.materialization = context().actorOf(GraphMaterialization.props(logicalGraph, barrier));

    if (localRange.equals(ackerRange)) {
      localAcker = context().actorOf(Acker.props((frontId, attachTimestamp) -> {}), "acker");
    } else {
      localAcker = null;
    }
  }

  public static Props props(IntRange localRange, Graph logicalGraph, Map<IntRange, ActorRef> managers) {
    return Props.create(LogicGraphManager.class, localRange, logicalGraph, managers);
  }

  @Override
  public void preStart() throws Exception {
    resolvedAcker().thenAcceptBoth(
            resolvedRoutes(),
            (ackerRef, router) -> {
              final FlameRoutes flameRoutes = new FlameRoutes(ackerRef, router);
              materialization.tell(flameRoutes, self());
              negotiator.tell(new NewMaterialization(materialization, ackerRef), self());
              if (localAcker != null) {
                localAcker.tell(flameRoutes, self());
              }
            }
    );
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(
                    String.class,
                    s -> s.equals("gimmeMaterialization"),
                    s -> sender().tell(new RangeMaterialization(localRange, materialization), self())
            )
            .match(
                    String.class,
                    s -> s.equals("gimmeAcker"),
                    s -> {
                      if (localAcker != null) {
                        sender().tell(localAcker, self());
                      } else {
                        unhandled(s);
                      }
                    }
            )
            .build();
  }

  private CompletionStage<ActorRef> resolvedAcker() {
    return PatternsCS.ask(
            managers.get(ackerRange),
            "gimmeAcker",
            Timeout.apply(10, TimeUnit.SECONDS)
    ).thenApply(response -> (ActorRef) response);
  }

  private CompletionStage<CoarseRouter> resolvedRoutes() {
    return managers.entrySet()
            .stream()
            .map(e -> PatternsCS.ask(
                    e.getValue(),
                    "gimmeMaterialization",
                    Timeout.apply(10, TimeUnit.SECONDS)
                    ).thenApply(answer -> (RangeMaterialization) answer)
            )
            .reduce(
                    CompletableFuture.completedFuture(new HashMap<IntRange, ActorRef>()),
                    (mapStage, rangeStage) -> mapStage.thenCombine(rangeStage, (map, range) -> {
                      map.put(range.range(), range.materialization());
                      return map;
                    }),
                    (aStage, bStage) -> aStage.thenCombine(
                            bStage,
                            (a, b) -> {
                              a.putAll(b);
                              return a;
                            }
                    )
            )
            .thenApply(ranges -> new CoarseRouter(logicalGraph, ranges));
  }

  public static class CoarseRouter implements GraphRouter {
    private final Graph graph;
    private final IntRangeMap<ActorRef> hashRanges;

    public CoarseRouter(Graph graph,
                        Map<IntRange, ActorRef> hashRanges) {
      this.graph = graph;
      this.hashRanges = new ListIntRangeMap<>(hashRanges);
    }

    @Override
    public void tell(DataItem<?> message, Graph.Vertex destanation, ActorRef sender) {
      // TODO: 11/28/17
    }

    @Override
    public void broadcast(Object message, ActorRef sender) {
      hashRanges.values().forEach(v -> v.tell(message, sender));
    }
  }
}
