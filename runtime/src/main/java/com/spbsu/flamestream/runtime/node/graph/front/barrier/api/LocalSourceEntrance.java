package com.spbsu.flamestream.runtime.node.graph.front.barrier.api;

import akka.actor.ActorRef;

public class LocalSourceEntrance {
  private final String graphId;

  private final ActorRef source;
  private final ActorRef acker;

  public LocalSourceEntrance(ActorRef source, String graphId, ActorRef acker) {
    this.source = source;
    this.graphId = graphId;
    this.acker = acker;
  }

  public ActorRef acker() {
    return acker;
  }

  public String graphId() {
    return graphId;
  }

  public ActorRef source() {
    return source;
  }
}
