package com.spbsu.flamestream.runtime.node.edge;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.node.edge.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class EdgeManager extends LoggingActor {
  private EdgeManager() {
    context().actorOf(Negotiator.props(), "negotiator");
  }

  public static Props props() {
    return Props.create(EdgeManager.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .build();
  }

}
