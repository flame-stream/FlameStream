package com.spbsu.flamestream.runtime.negitioator.api;

import akka.actor.ActorRef;

public class AttachSource {
  private final ActorRef source;

  public AttachSource(ActorRef source) {
    this.source = source;
  }

  public ActorRef source() {
    return source;
  }
}
