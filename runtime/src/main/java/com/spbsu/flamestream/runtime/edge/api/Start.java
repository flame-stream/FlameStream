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

  public ActorRef hole() {
    return consumer;
  }

  public GlobalTime from() {
    return globalTime;
  }

  @Override
  public String toString() {
    return "Start{" +
            "consumer=" + consumer +
            ", globalTime=" + globalTime +
            '}';
  }
}
