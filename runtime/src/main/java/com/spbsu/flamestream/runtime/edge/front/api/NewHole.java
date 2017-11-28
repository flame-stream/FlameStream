package com.spbsu.flamestream.runtime.edge.front.api;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class NewHole {
  private final ActorRef source;
  private final GlobalTime lower;

  public NewHole(ActorRef source, GlobalTime lower) {
    this.source = source;
    this.lower = lower;
  }

  public ActorRef source() {
    return source;
  }

  public GlobalTime lower() {
    return lower;
  }
}
