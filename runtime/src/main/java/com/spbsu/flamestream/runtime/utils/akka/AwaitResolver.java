package com.spbsu.flamestream.runtime.utils.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Identify;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class AwaitResolver extends AbstractActor {
  private ActorRef requester;
  private ActorPath request;

  public static Props props() {
    return Props.create(AwaitResolver.class);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(ActorPath.class, path -> {
              requester = sender();
              request = path;
              context().actorSelection(path).tell(new Identify(path), self());
              getContext().become(resolving());
            })
            .build();
  }

  private Receive resolving() {
    return ReceiveBuilder.create()
            .match(
                    ActorIdentity.class,
                    id -> id.getActorRef().isPresent(),
                    id -> {
                      requester.tell(id.getActorRef().get(), self());
                      context().stop(self());
                    }
            )
            .match(
                    ActorIdentity.class,
                    id -> !id.getActorRef().isPresent(),
                    id -> context().system().scheduler().scheduleOnce(
                            Duration.create(10, TimeUnit.MILLISECONDS),
                            () -> context().actorSelection(request).tell(new Identify(request), self()),
                            context().dispatcher()
                    )
            )
            .build();

  }
}
