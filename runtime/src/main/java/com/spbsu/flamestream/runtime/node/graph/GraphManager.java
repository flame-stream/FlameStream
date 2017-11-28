package com.spbsu.flamestream.runtime.node.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.graph.acker.Acker;
import com.spbsu.flamestream.runtime.node.graph.api.RangeMaterialization;
import com.spbsu.flamestream.runtime.node.graph.edge.EdgeManager;
import com.spbsu.flamestream.runtime.node.graph.materialization.GraphMaterialization;
import com.spbsu.flamestream.runtime.node.graph.materialization.GraphRouter;
import com.spbsu.flamestream.runtime.node.graph.materialization.ops.Materialization;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class GraphManager extends LoggingActor {
  private final IntRange localRange;
  private final IntRange ackerLocation;

  private final Graph logicalGraph;
  private final Map<IntRange, ActorRef> managers;

  private final ActorRef edgeManager;
  private final ActorRef materialization;

  private GraphManager(IntRange localRange,
                       Graph logicalGraph,
                       Map<IntRange, ActorRef> managers,
                       IntRange ackerLocation) {
    this.ackerLocation = ackerLocation;
    this.localRange = localRange;
    this.logicalGraph = logicalGraph;
    this.managers = managers;
    this.edgeManager = context().actorOf(EdgeManager.props());
    this.materialization = context().actorOf(GraphMaterialization.props(logicalGraph, ))
    if (ackerLocation.equals(localRange)) {
      context().actorOf(Acker.props((frontId, attachTimestamp) -> {}), "acker");
    }
  }

  public static Props props(IntRange localRange, Graph logicalGraph, Map<IntRange, ActorRef> managers) {
    return Props.create(GraphManager.class, localRange, logicalGraph, managers);
  }

  @Override
  public void preStart() throws Exception {
    final CompletionStage<ActorRef> acker = PatternsCS.ask(
            managers.get(ackerLocation),
            "gimmeAcker",
            Timeout.apply(10, TimeUnit.SECONDS)
    ).thenApply(response -> (ActorRef) response);

    final CompletionStage<ActorRef> materialization = acker.thenApply(a -> context().actorOf(
            GraphMaterialization.props(logicalGraph, a, context().system().deadLetters()),
            "graph"
            )
    );

    materialization.thenAcceptBoth(resolvedRoutes(), (m, router) -> m.tell(router, self()));
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
            .build();
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
