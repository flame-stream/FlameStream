package com.spbsu.flamestream.runtime.node.front;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class FrontManager extends LoggingActor {
  private FrontManager() {
  }

  public static Props props() {
    return Props.create(FrontManager.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FlameRuntime.Front.class, front -> context().actorOf(FrontActor.props(front)))
            .build();
  }
}
