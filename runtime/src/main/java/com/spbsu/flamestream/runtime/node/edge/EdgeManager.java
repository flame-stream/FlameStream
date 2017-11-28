package com.spbsu.flamestream.runtime.node.edge;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.node.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.node.edge.front.FrontActor;
import com.spbsu.flamestream.runtime.node.graph.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.node.negitioator.api.AttachFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class EdgeManager extends LoggingActor {
  private final ActorRef negoriator;

  private EdgeManager(ActorRef negoriator) {
    this.negoriator = negoriator;
  }

  public static Props props(ActorRef negoriator) {
    return Props.create(EdgeManager.class, negoriator);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FrontInstance.class, frontInstance -> {
              final ActorRef frontRef = context().actorOf(FrontActor.props(frontInstance), frontInstance.id());
              negoriator.tell(new RegisterFront(frontInstance.id(), frontRef), self());
            })
            .build();
  }
}
