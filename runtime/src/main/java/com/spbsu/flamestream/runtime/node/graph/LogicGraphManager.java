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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LogicGraphManager extends LoggingActor {
  private final Graph logicalGraph;

  private final IntRange localRange;
  private final Map<IntRange, ActorRef> managers;
  private final ActorRef materialization;
  private final ActorRef negotiator;
  private final ActorRef barrier;

  private final ActorRef acker;

  private LogicGraphManager(Graph logicalGraph,
                            ActorRef negotiator,
                            ActorRef barrier,
                            IntRange localRange,
                            IntRange ackerLocation,
                            Map<IntRange, ActorRef> managers) throws ExecutionException, InterruptedException {
    this.localRange = localRange;
    this.logicalGraph = logicalGraph;
    this.negotiator = negotiator;
    this.barrier = barrier;
    this.managers = managers;
    this.materialization = context().actorOf(GraphMaterialization.props(logicalGraph, barrier));

    if (localRange.equals(ackerLocation)) {
      acker = context().actorOf(Acker.props((frontId, attachTimestamp) -> {}), "acker");
    } else {
      acker = PatternsCS.ask(
              managers.get(ackerLocation),
              "gimmeAcker",
              Timeout.apply(10, TimeUnit.SECONDS)
      ).thenApply(response -> (ActorRef) response).toCompletableFuture().get();
    }
  }

  public static Props props(Graph logicalGraph,
                            IntRange localRange,
                            IntRange ackerLocation,
                            Map<IntRange, ActorRef> managers,
                            ActorRef negotiator,
                            ActorRef barrier) {
    return Props.create(LogicGraphManager.class, logicalGraph, localRange, ackerLocation, managers, negotiator, barrier);
  }

  @Override
  public void preStart() throws Exception {
    resolvedAcker().thenAcceptBoth(
            resolvedRoutes(),
            (ackerRef, router) -> {
              final FlameRoutes flameRoutes = new FlameRoutes(ackerRef, router);
              materialization.tell(flameRoutes, self());
              negotiator.tell(new NewMaterialization(materialization, ackerRef), self());
              if (acker != null) {
                acker.tell(flameRoutes, self());
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
                      if (acker != null) {
                        sender().tell(acker, self());
                      } else {
                        unhandled(s);
                      }
                    }
            )
            .build();
  }

  private CompletionStage<ActorRef> resolvedAcker() {
    return
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
    private final IntRangeMap<ActorRef> managers;
    private final IntRangeMap<ActorRef> hashRanges;

    public CoarseRouter(Graph graph,
                        IntRangeMap<ActorRef> managers,
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
