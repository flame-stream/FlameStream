package com.spbsu.flamestream.runtime.node.graph.materialization;

import akka.actor.ActorRef;

public class FlameRoutes {
  private final ActorRef acker;
  private final GraphRouter router;

  public FlameRoutes(ActorRef acker, GraphRouter router) {
    this.acker = acker;
    this.router = router;
  }

  public ActorRef acker() {
    return acker;
  }

  public GraphRouter router() {
    return router;
  }
}
