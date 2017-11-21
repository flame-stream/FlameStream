package com.spbsu.flamestream.runtime.node.materializer;

import akka.actor.ActorRef;
import akka.routing.Router;

public class GraphRoutes {
  private final Router graphRouter;
  private final ActorRef acker;

  public GraphRoutes(Router graphRouter,
                     ActorRef acker) {
    this.graphRouter = graphRouter;
    this.acker = acker;
  }

  public Router graphRouter() {
    return graphRouter;
  }

  public ActorRef acker() {
    return acker;
  }
}
