package com.spbsu.flamestream.runtime.barrier;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class Barrier extends LoggingActor {

  public static Props props() {
    return Props.create(Barrier.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create().build();
  }
}
