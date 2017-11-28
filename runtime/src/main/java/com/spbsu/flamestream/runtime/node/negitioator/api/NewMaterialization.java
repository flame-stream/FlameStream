package com.spbsu.flamestream.runtime.node.negitioator.api;

import akka.actor.ActorRef;

public class NewMaterialization {
  private final ActorRef source;
  private final ActorRef acker;

  public NewMaterialization(ActorRef source, ActorRef acker) {
    this.source = source;
    this.acker = acker;
  }

  public ActorRef source() {
    return source;
  }

  public ActorRef acker() {
    return acker;
  }
}
