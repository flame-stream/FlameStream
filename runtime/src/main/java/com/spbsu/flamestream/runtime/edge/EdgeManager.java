package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.edge.front.FrontActor;
import com.spbsu.flamestream.runtime.negitioator.api.AttachFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class EdgeManager extends LoggingActor {
  private final ActorRef negotiator;
  private final String nodeId;

  private EdgeManager(String nodeId, ActorRef negoriator) {
    this.nodeId = nodeId;
    this.negotiator = negoriator;
  }

  public static Props props(String nodeId, ActorRef negotiator) {
    return Props.create(EdgeManager.class, nodeId, negotiator);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FrontInstance.class, frontInstance -> {
              final ActorRef frontRef = context().actorOf(FrontActor.props(nodeId, frontInstance), frontInstance.id());
              negotiator.tell(new AttachFront(frontInstance.id(), frontRef), self());
            })
            .build();
  }
}
