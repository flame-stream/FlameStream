package com.spbsu.flamestream.runtime.node.graph.api;

import akka.actor.ActorRef;
import com.spbsu.flamestream.runtime.node.graph.GraphRouter;

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
