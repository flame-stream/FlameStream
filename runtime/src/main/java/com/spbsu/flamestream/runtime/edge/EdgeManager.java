package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.negitioator.api.NewFront;
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
            .match(AttachFront.class, attachFront -> {
              final ActorRef frontRef = context().actorOf(FrontActor.props(
                      new EdgeId(attachFront.id(), nodeId),
                      attachFront.instance()
              ), attachFront.id());
              negotiator.tell(new NewFront(new EdgeId(attachFront.id(), nodeId), frontRef), self());
            })
            .match(AttachRear.class, attachRear -> {
              final ActorRef rearRef = context().actorOf(RearActor.props(
                      new EdgeId(attachRear.id(), nodeId),
                      attachRear.instance()
              ), attachRear.id());
              barrier.tell(new com.spbsu.flamestream.runtime.barrier.api.AttachRear(rearRef), self());
            })
            .build();
  }
}
