package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;
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
  private final ActorPath nodePath;
  private final String nodeId;
  private final ActorRef negotiator;
  private final ActorRef barrier;

  private EdgeManager(ActorPath nodePath, String nodeId, ActorRef localNegotiator, ActorRef localBarrier) {
    this.nodeId = nodeId;
    this.nodePath = nodePath;
    this.negotiator = localNegotiator;
    this.barrier = localBarrier;
  }

  public static Props props(ActorPath nodePath, String nodeId, ActorRef localNegotiator, ActorRef localBarrier) {
    return Props.create(EdgeManager.class, nodePath, nodeId, localNegotiator, localBarrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FrontInstance.class, frontInstance -> {
              final ActorRef frontRef = context().actorOf(FrontActor.props(
                      new SystemEdgeContext(nodePath, nodeId, frontInstance.id(), context()),
                      frontInstance
              ), frontInstance.id());
              negotiator.tell(new AttachFront(frontInstance.id(), frontRef), self());
            })
            .match(RearInstance.class, rearInstance -> {
              final ActorRef rearRef = context().actorOf(RearActor.props(
                      new SystemEdgeContext(nodePath, nodeId, rearInstance.id(), context()),
                      rearInstance
              ), rearInstance.id());
              barrier.tell(new AttachRear(rearRef), self());
            })
            .build();
  }
}
