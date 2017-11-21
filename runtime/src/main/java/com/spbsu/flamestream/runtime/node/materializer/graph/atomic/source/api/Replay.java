package com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.api;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

public final class Replay {
  private final ActorRef hole;
  private final GlobalTime from;
  private final GlobalTime to;

  public Replay(ActorRef hole, GlobalTime from, GlobalTime to) {
    this.hole = hole;
    this.from = from;
    this.to = to;
  }

  public ActorRef hole() {
    return hole;
  }

  public GlobalTime from() {
    return from;
  }

  public GlobalTime to() {
    return to;
  }
}

