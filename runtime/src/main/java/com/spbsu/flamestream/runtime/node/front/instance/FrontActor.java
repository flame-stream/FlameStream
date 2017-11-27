package com.spbsu.flamestream.runtime.node.front.instance;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.node.front.api.FrontInstance;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

class FrontActor extends LoggingActor {
  private final FlameRuntime.Front front;
  private final String id;

  private FrontActor(FrontInstance front) {
    this.front = front.front();
    this.id = front.frontId();
  }

  static Props props(FrontInstance front) {
    return Props.create(FrontActor.class, front);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .build();
  }
}
