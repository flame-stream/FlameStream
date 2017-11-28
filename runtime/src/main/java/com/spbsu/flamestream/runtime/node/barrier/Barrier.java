package com.spbsu.flamestream.runtime.node.barrier;

import akka.actor.Props;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class Barrier extends LoggingActor {

  public static Props props() {
    return Props.create(Barrier.class);
  }

  @Override
  public Receive createReceive() {
    return null;
  }
}
