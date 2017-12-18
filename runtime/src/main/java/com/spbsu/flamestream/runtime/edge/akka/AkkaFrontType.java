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
import java.util.stream.Stream;

public class AkkaFrontType<T> implements FlameRuntime.FrontType<AkkaFront, AkkaFrontType.Handle<T>> {
  private final ActorSystem system;

  public AkkaFrontType(ActorSystem system) {
    this.system = system;
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
    final ActorRef ref = AwaitResolver.syncResolve(
            context.nodePath().child("edge").child(context.edgeId() + "-inner"),
            system
    );
    return new Handle<>(ref, system);
  }

  public static class Handle<T> implements Consumer<T> {
    static final Timeout TIMEOUT = new Timeout(60, TimeUnit.SECONDS);
    private final ActorRef innerActor;

    Handle(ActorRef frontRef, ActorSystem system) {
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

    public void accept(Stream<T> stream) {
      final CompletionStage<Object> stage = PatternsCS.ask(innerActor, stream, TIMEOUT);
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
              .match(Stream.class, this::onStream)
              .match(Next.class, next -> onRequestNext())
              .build();
    }

    private void onRawData(RawData rawData) {
      frontActor.tell(rawData, self());
      sender = sender();
    }

    private void onRequestNext() {
      if (sender != null) {
        sender.tell("Unlock", self());
        sender = null;
      }
    }

    private void onStream(Stream<?> stream) {
      stream.forEach(o -> frontActor.tell(new RawData<>(o), self()));
      sender = sender();
    }
  }

  static class Next {
  }
}

