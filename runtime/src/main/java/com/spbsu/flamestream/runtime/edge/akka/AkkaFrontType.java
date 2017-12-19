package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AkkaFrontType<T> implements FlameRuntime.FrontType<AkkaFront, Consumer<T>> {
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
  public Consumer<T> handle(EdgeContext context) {
    final ActorRef frontRef = AwaitResolver.syncResolve(
            context.nodePath().child("edge").child(context.edgeId() + "-inner"),
            system
    );
    return backPressure ? new BackPressureHandle<>(frontRef, system) : new SimpleHandle<>(frontRef);
  }

  public static class SimpleHandle<T> implements Consumer<T> {
    private final ActorRef frontRef;

    SimpleHandle(ActorRef frontRef) {
      this.frontRef = frontRef;
    }

    @Override
    public void accept(T o) {
      frontRef.tell(new RawData<>(o), ActorRef.noSender());
    }
  }

  public static class BackPressureHandle<T> implements Consumer<T> {
    static final Timeout TIMEOUT = new Timeout(60, TimeUnit.SECONDS);
    private final ActorRef innerActor;

    BackPressureHandle(ActorRef frontRef, ActorSystem system) {
      innerActor = system.actorOf(InnerActor.props(frontRef));
    }

    @Override
    public void accept(T o) {
      final CompletionStage<Object> stage = PatternsCS.ask(innerActor, new RawData<>(o), TIMEOUT);
      try {
        stage.toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
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
              .match(RawData.class, this::onRawData)
              .match(Next.class, next -> onRequestNext())
              .build();
    }

    private void onRawData(RawData rawData) {
      frontActor.tell(rawData, self());
      sender = sender();
    }

    private void onRequestNext() {
      sender.tell("Unlock", self());
    }
  }

  static class Next {
  }
}

