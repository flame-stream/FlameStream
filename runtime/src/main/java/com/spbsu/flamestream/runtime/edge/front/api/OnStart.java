package com.spbsu.flamestream.runtime.edge.front.api;

import akka.actor.ActorRef;

public class OnStart {
  private final ActorRef consumer;

  public OnStart(ActorRef consumer) {
    this.consumer = consumer;
  }

  public ActorRef consumer() {
    return consumer;
  }
}
