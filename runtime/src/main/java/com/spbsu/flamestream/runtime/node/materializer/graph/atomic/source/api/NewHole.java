package com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.api;

import akka.actor.ActorRef;

public final class NewHole {
  private final long startTs;
  private final ActorRef hole;

  public NewHole(ActorRef hole, long startTs) {
    this.hole = hole;
    this.startTs = startTs;
  }

  public ActorRef hole() {
    return hole;
  }

  public long startTs() {
    return this.startTs;
  }
}
