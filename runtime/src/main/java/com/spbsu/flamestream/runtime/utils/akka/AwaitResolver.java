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
import com.spbsu.flamestream.runtime.utils.FlameConfig;
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
      return resolve(path, context).toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to resolve " + path, e);
    }
  }

  public static CompletionStage<ActorRef> resolve(ActorPath path, ActorRefFactory context) {
    final ActorRef resolver = context.actorOf(AwaitResolver.props());
    return PatternsCS.ask(resolver, path, FlameConfig.config.bigTimeout())
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
                      //noinspection ConstantConditions
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
