package com.spbsu.flamestream.runtime.node.negitioator.api;

import akka.actor.ActorRef;

public class AttachFront {
  private final String frontId;
  private final ActorRef front;

  public AttachFront(String frontId, ActorRef front) {
    this.frontId = frontId;
    this.front = front;
  }

  public String frontId() {
    return frontId;
  }

  public ActorRef front() {
    return front;
  }
}
