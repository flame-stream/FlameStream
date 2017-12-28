package com.spbsu.flamestream.runtime.edge.api;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class Start {
  private final ActorRef consumer;
  private final GlobalTime globalTime;

  public Start(ActorRef consumer, GlobalTime globalTime) {
    this.consumer = consumer;
    this.globalTime = globalTime;
  }

  public ActorRef consumer() {
    return consumer;
  }

  public GlobalTime globalTime() {
    return this.globalTime;
  }
}
