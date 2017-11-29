package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.barrier.api.AttachRear;
import com.spbsu.flamestream.runtime.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.edge.api.RearInstance;
import com.spbsu.flamestream.runtime.edge.front.FrontActor;
import com.spbsu.flamestream.runtime.edge.rear.RearActor;
import com.spbsu.flamestream.runtime.negitioator.api.AttachFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class EdgeManager extends LoggingActor {
  private final String nodeId;
  private final ActorRef negotiator;
  private final ActorRef barrier;

  private EdgeManager(String nodeId, ActorRef localNegotiator, ActorRef localBarrier) {
    this.nodeId = nodeId;
    this.negotiator = localNegotiator;
    this.barrier = localBarrier;
  }

  public static Props props(String nodeId, ActorRef localNegotiator, ActorRef localBarrier) {
    return Props.create(EdgeManager.class, nodeId, localNegotiator, localBarrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FrontInstance.class, frontInstance -> {
              final ActorRef frontRef = context().actorOf(FrontActor.props(nodeId, frontInstance), frontInstance.name());
              negotiator.tell(new AttachFront(frontInstance.name(), frontRef), self());
            })
            .match(RearInstance.class, rearInstance -> {
              final ActorRef rearRef = context().actorOf(RearActor.props(rearInstance), rearInstance.rearId());
              barrier.tell(new AttachRear(rearRef), self());
            })
            .build();
  }
}
