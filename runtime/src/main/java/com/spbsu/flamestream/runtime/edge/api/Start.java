package com.spbsu.flamestream.runtime.edge.api;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class Start {
  private final ActorRef consumer;

  public Start(ActorRef consumer) {
    this.consumer = consumer;
  }

  public ActorRef hole() {
    return consumer;
  }

  @Override
  public String toString() {
    return "Start{" +
            "consumer=" + consumer +
            '}';
  }
}
