package com.spbsu.flamestream.runtime.node.materializer;

import akka.actor.ActorRef;
import com.spbsu.flamestream.runtime.node.materializer.router.FlameRouter;

public class GraphRoutes {
  private final ActorRef acker;
  private final FlameRouter router;

  public GraphRoutes(FlameRouter router,
                     ActorRef acker) {
    this.router = router;
    this.acker = acker;
  }

  public FlameRouter router() {
    return router;
  }

  public ActorRef acker() {
    return acker;
  }
}
