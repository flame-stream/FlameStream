package com.spbsu.flamestream.runtime.negitioator.api;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.EdgeInstance;

public class NewFront {
  private final EdgeInstance frontInstance;
  private final ActorRef front;

  public NewFront(EdgeInstance frontInstance, ActorRef front) {
    this.frontInstance = frontInstance;
    this.front = front;
  }

  public EdgeInstance frontInstance() {
    return frontInstance;
  }

  public ActorRef front() {
    return front;
  }
}
