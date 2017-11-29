package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.FlameNode;
import com.spbsu.flamestream.runtime.graph.materialization.ActorPerNodeGraphMaterializer;
import com.spbsu.flamestream.runtime.graph.materialization.GraphMaterialization;
import com.spbsu.flamestream.runtime.graph.materialization.GraphMaterializer;
import com.spbsu.flamestream.runtime.graph.materialization.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class LogicGraphManager extends LoggingActor {
  private final Graph logicalGraph;
  private final ActorRef acker;
  private final BarrierRouter barrier;

  private GraphMaterializer materializer;
  private GraphMaterialization materialization;

  private LogicGraphManager(Graph logicalGraph,
                            ActorRef acker,
                            BarrierRouter barrier) {
    this.logicalGraph = logicalGraph;
    this.acker = acker;
    this.barrier = barrier;
  }

  public static Props props(Graph logicalGraph,
                            ActorRef acker,
                            BarrierRouter barrier) {
    return Props.create(LogicGraphManager.class, logicalGraph, acker, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FlameRouter.class, router -> {
              log().info("Received range map, completing constructor");
              materializer = new ActorPerNodeGraphMaterializer(router, acker, barrier);
              materialization = materializer.materialize(logicalGraph);
              unstashAll();
              getContext().become(managing());
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive managing() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, item -> materialization.inject(item))
            .match(DataItem.class, item -> materialization.accept(item))
            .build();
  }
}
