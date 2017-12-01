package com.spbsu.flamestream.runtime.edge.rear;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

public class ActorRear extends LoggingActor implements Rear {
  private final String path;

  private ActorRef consumer;

  private ActorRear(String path) {
    this.path = path;
  }

  public static Props props(String path) {
    return Props.create(ActorRear.class, path);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    consumer = context().actorSelection(path)
            .resolveOneCS(FiniteDuration.apply(10, TimeUnit.SECONDS))
            .toCompletableFuture()
            .get();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .matchAny(this::accept)
            .build();
  }


  @Override
  public void accept(Object o) {
    consumer.tell(o, self());
  }
}
