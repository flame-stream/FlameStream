package com.spbsu.flamestream.runtime.node.graph.front;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.node.graph.front.barrier.FrontBarrier;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class FrontManager extends LoggingActor {
  private FrontManager() {
    context().actorOf(FrontBarrier.props(), "barrier");
  }

  public static Props props() {
    return Props.create(FrontManager.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .build();
  }

}
