package com.spbsu.flamestream.runtime.node.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.graph.materialization.GraphMaterialization;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.AddressedItem;
import com.spbsu.flamestream.runtime.node.negitioator.api.NewMaterialization;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;

public class LogicGraphManager extends LoggingActor {
  private final Graph logicalGraph;
  private final ActorRef acker;
  private final ActorRef negotiator;
  private final ActorRef barrier;

  private ActorRef materialization;
  private IntRangeMap<ActorRef> managers;


  private LogicGraphManager(Graph logicalGraph,
                            ActorRef acker,
                            ActorRef negotiator,
                            ActorRef barrier) {
    this.logicalGraph = logicalGraph;
    this.acker = acker;
    this.negotiator = negotiator;
    this.barrier = barrier;
  }

  public static Props props(Graph logicalGraph,
                            ActorRef acker,
                            ActorRef negotiator,
                            ActorRef barrier) {
    return Props.create(LogicGraphManager.class, logicalGraph, acker, negotiator, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(IntRangeMap.class, managers -> {
              this.managers = managers;
              this.materialization = context().actorOf(GraphMaterialization.props(
                      logicalGraph,
                      new CoarseRouter(logicalGraph, managers),
                      acker,
                      barrier
              ), "graph");
              negotiator.tell(new NewMaterialization(materialization), self());
              unstashAll();
              getContext().become(managing());
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive managing() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, item -> materialization.forward(item, context()))
            .build();
  }


  public static class CoarseRouter implements GraphRouter {
    private final Graph graph;
    private final IntRangeMap<ActorRef> hashRanges;

    public CoarseRouter(Graph graph, IntRangeMap<ActorRef> routes) {
      this.graph = graph;
      this.hashRanges = routes;
    }

    @Override
    public void tell(AddressedItem item, ActorRef sender) {
      // TODO: 11/28/17 Hashing logic
    }

    @Override
    public void broadcast(Object message, ActorRef sender) {
      hashRanges.values().forEach(v -> v.tell(message, sender));
    }
  }
}
