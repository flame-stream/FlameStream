package com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.api;

import akka.actor.ActorRef;

public final class NewHole {
  private final ActorRef hole;

  public NewHole(ActorRef hole) {
    this.hole = hole;
  }

  public ActorRef hole() {
    return hole;
  }
}
