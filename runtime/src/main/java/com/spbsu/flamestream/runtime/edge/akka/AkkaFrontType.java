package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AkkaFrontType<T> implements FlameRuntime.FrontType<AkkaFront, AkkaFrontType.Handle<T>> {
  private final ActorSystem system;
  private final boolean backPressure;

  public AkkaFrontType(ActorSystem system, boolean backPressure) {
    this.system = system;
    this.backPressure = backPressure;
  }

  @Override
  public FlameRuntime.FrontInstance<AkkaFront> instance() {
    return new FlameRuntime.FrontInstance<AkkaFront>() {
      @Override
      public Class<AkkaFront> clazz() {
        return AkkaFront.class;
      }

      @Override
      public String[] params() {
        return new String[0];
      }
    };
  }

  @Override
  public Handle<T> handle(EdgeContext context) {
    final ActorRef frontRef = AwaitResolver.syncResolve(
            context.nodePath()
                    .child("edge")
                    .child(context.edgeId().edgeName())
                    .child(context.edgeId().nodeId() + "-inner"),
            system
    );
    return backPressure ? new BackPressureHandle<>(frontRef, system) : new SimpleHandle<>(frontRef);
  }

  private static class SimpleHandle<T> extends Handle<T> {
    SimpleHandle(ActorRef frontRef) {
      super(frontRef);
    }

    @Override
    public void accept(T o) {
      frontRef.tell(new AkkaFront.RawData<>(o), ActorRef.noSender());
    }
  }

  private static class BackPressureHandle<T> extends Handle<T> {
    static final Timeout TIMEOUT = new Timeout(60, TimeUnit.SECONDS);
    private final ActorRef innerActor;

    BackPressureHandle(ActorRef frontRef, ActorSystem system) {
      super(frontRef);
      innerActor = system.actorOf(InnerActor.props(frontRef));
    }

    @Override
    public void accept(T o) {
      final CompletionStage<Object> stage = PatternsCS.ask(innerActor, new AkkaFront.RawData<>(o), TIMEOUT);
      try {
        stage.toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static abstract class Handle<T> implements Consumer<T> {
    final ActorRef frontRef;

    Handle(ActorRef frontRef) {
      this.frontRef = frontRef;
    }

    public void eos() {
      frontRef.tell(new AkkaFront.EOS(), ActorRef.noSender());
    }
  }

  private static class InnerActor extends LoggingActor {
    private final ActorRef frontActor;
    private ActorRef sender = null;

    private InnerActor(ActorRef frontActor) {
      this.frontActor = frontActor;
    }

    public static Props props(ActorRef ref) {
      return Props.create(InnerActor.class, ref);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(AkkaFront.RawData.class, this::onRawData)
              .match(RequestNext.class, next -> onRequestNext())
              .build();
    }

    private void onRawData(AkkaFront.RawData rawData) {
      frontActor.tell(rawData, self());
      sender = sender();
    }

    private void onRequestNext() {
      sender.tell("Unlock", self());
    }
  }
}

