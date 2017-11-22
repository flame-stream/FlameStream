package com.spbsu.flamestream.runtime.node.front.barrier.api;

import akka.actor.ActorRef;

public class LocalFront {
  private final String frontId;
  private final ActorRef front;

  public LocalFront(String frontId, ActorRef front) {
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
