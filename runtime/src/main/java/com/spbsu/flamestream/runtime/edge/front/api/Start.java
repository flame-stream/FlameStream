package com.spbsu.flamestream.runtime.edge.front.api;

import akka.actor.ActorRef;

public class Start {
  private final ActorRef consumer;

  public Start(ActorRef consumer) {
    this.consumer = consumer;
  }

  public ActorRef consumer() {
    return consumer;
  }
}
