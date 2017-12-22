package com.spbsu.flamestream.runtime.utils.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Identify;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AwaitResolver extends AbstractActor {
  private ActorRef requester;
  private ActorPath request;

  public static Props props() {
    return Props.create(AwaitResolver.class);
  }

  public static ActorRef syncResolve(ActorPath path, ActorRefFactory context) {
    try {
      return resolve(path, context).toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException("Failed to resolve " + path, e);
    }
  }

  public static CompletionStage<ActorRef> resolve(ActorPath path, ActorRefFactory context) {
    final ActorRef resolver = context.actorOf(AwaitResolver.props().withDispatcher("resolver-dispatcher"));
    return PatternsCS.ask(resolver, path, Timeout.apply(100, TimeUnit.SECONDS))
            .thenApply(a -> (ActorRef) a);
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
                            Duration.create(100, TimeUnit.MILLISECONDS),
                            () -> context().actorSelection(request).tell(new Identify(request), self()),
                            context().dispatcher()
                    )
            )
            .build();

  }
}
