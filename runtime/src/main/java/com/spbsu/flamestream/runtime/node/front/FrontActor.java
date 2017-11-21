package com.spbsu.flamestream.runtime.node.front;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

class FrontActor extends LoggingActor {
  private final FlameRuntime.Front front;

  private FrontActor(FlameRuntime.Front front) {
    this.front = front;
  }

  static Props props(FlameRuntime.Front front) {
    return Props.create(FrontActor.class, front);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .build();
  }
}
