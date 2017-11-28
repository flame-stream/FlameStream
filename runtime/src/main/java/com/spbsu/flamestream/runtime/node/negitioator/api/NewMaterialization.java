package com.spbsu.flamestream.runtime.node.negitioator.api;

import akka.actor.ActorRef;

public class NewMaterialization {
  private final ActorRef source;

  public NewMaterialization(ActorRef source) {
    this.source = source;
  }

  public ActorRef source() {
    return source;
  }
}
