package com.spbsu.flamestream.runtime.negitioator.api;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.EdgeId;

public class NewFront {
  private final EdgeId frontId;
  private final ActorRef front;

  public NewFront(EdgeId frontId, ActorRef front) {
    this.frontId = frontId;
    this.front = front;
  }

  public EdgeId frontId() {
    return frontId;
  }

  public ActorRef front() {
    return front;
  }
}
