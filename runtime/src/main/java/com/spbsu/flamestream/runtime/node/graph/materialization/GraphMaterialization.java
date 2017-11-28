package com.spbsu.flamestream.runtime.node.graph.materialization;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.graph.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.AddressedItem;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.Commit;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class GraphMaterialization extends LoggingActor {
  private final Graph graph;
  private final ActorRef acker;
  private final ActorRef barrier;

  private GraphMaterialization(Graph graph, ActorRef acker, ActorRef barrier) {
    this.graph = graph;
    this.acker = acker;
    this.barrier = barrier;
  }

  public static Props props(Graph graph, ActorRef acker, ActorRef barrier) {
    return Props.create(GraphMaterialization.class, graph, acker, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(GraphRouter.class, routes -> {
              unstashAll();
              getContext().become(ranging());
            })
            .matchAny(m -> stash()).build();
  }

  private Receive ranging() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, this::onAddressedItem)
            .match(MinTimeUpdate.class, this::onMinTimeUpdate)
            .match(Commit.class, this::onCommit)
            .build();
  }

  private void onAddressedItem(AddressedItem atomicMessage) {

  }

  private void onMinTimeUpdate(Object message) {

  }

  private void onCommit(Commit commit) {

  }
}
