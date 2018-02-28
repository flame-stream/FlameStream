package com.spbsu.flamestream.runtime.graph.api;

import akka.actor.ActorRef;

public class NewRear {
  private final ActorRef rear;

  public NewRear(ActorRef rear) {
    this.rear = rear;
  }

  public ActorRef rear() {
    return rear;
  }
}
