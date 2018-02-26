package com.spbsu.flamestream.runtime.graph.api;

import akka.actor.ActorRef;

public class AttachRear {
  private final ActorRef rear;

  public AttachRear(ActorRef rear) {
    this.rear = rear;
  }

  public ActorRef rear() {
    return rear;
  }
}
