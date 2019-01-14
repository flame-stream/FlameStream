package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.runtime.edge.Rear;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.api.BatchAccepted;
import com.spbsu.flamestream.runtime.edge.api.GimmeLastBatch;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class AkkaRear implements Rear {
  private final ActorRef innerActor;

  public AkkaRear(EdgeContext edgeContext, ActorRefFactory refFactory, String localMediatorPath) {
    this.innerActor = refFactory.actorOf(
            RemoteMediator.props(localMediatorPath + '/' + edgeContext.edgeId().nodeId() + "-localrear"),
            edgeContext.edgeId().nodeId() + "-inner"
    );
  }

  @Override
  public void accept(Batch batch) {
    try {
      PatternsCS.ask(innerActor, batch, FlameConfig.config.smallTimeout()).toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Batch last() {
    try {
      return PatternsCS.ask(innerActor, new GimmeLastBatch(), FlameConfig.config.smallTimeout())
              .thenApply(Batch.class::cast)
              .toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static class RemoteMediator extends LoggingActor {
    private final String localMediatorPath;
    private ActorRef localMediator;

    private RemoteMediator(String localMediatorPath) {
      this.localMediatorPath = localMediatorPath;
    }

    public static Props props(String localMediatorPath) {
      return Props.create(RemoteMediator.class, localMediatorPath);
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      final ActorPath path = ActorPaths.fromString(localMediatorPath);
      localMediator = AwaitResolver.resolve(path, context())
              .toCompletableFuture()
              .get();
      log().info("Local mediator has been resolved");
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(Batch.class, b -> {
                System.out.format("AkkaRear.RemoteMediator <default> got Batch %s%n", b);
                final ActorRef sender = sender();
                PatternsCS.ask(localMediator, b, FlameConfig.config.smallTimeout())
                        .thenRun(() -> sender.tell(new BatchAccepted(), self()));
              })
              .match(GimmeLastBatch.class, g -> {
                final ActorRef sender = sender();
                PatternsCS.ask(localMediator, g, FlameConfig.config.smallTimeout())
                        .thenApply(a -> (Batch) a)
                        .thenAccept(batch -> sender.tell(batch, self()));
              })
              .build();
    }
  }

  public static class LocalMediator<T> extends LoggingActor {
    private final Class<T> clazz;
    private Consumer<T> consumer = null;

    private Batch lastBatch = Batch.Default.EMPTY;

    private LocalMediator(Class<T> clazz) {
      this.clazz = clazz;
    }

    public static <T> Props props(Class<T> clazz) {
      System.out.format("AkkaRear.LocalMediator %s%n", clazz);
      return Props.create(LocalMediator.class, clazz);
    }




    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(Consumer.class, c -> {
                //noinspection unchecked
                System.out.format("AkkaRear.LocalMediator <default> got consumer %s%n", c);
                this.consumer = c;
                unstashAll();
                getContext().become(serving());
              })
              .matchAny(a -> {
                System.out.format("AkkaRear.LocalMediator.receive %s%n", a);
                stash();
              })
              .build();
    }

    private Receive serving() {
      return ReceiveBuilder.create()
              .match(Consumer.class, c -> {
                //noinspection unchecked
                System.out.format("AkkaRear.LocalMediator <serving> got consumer %s%n", c);
                consumer = c;
              })
              .match(Batch.class, b -> {
                System.out.format("AkkaRear.LocalMediator <serving> got Batch %s %s%n", b.getClass(), b);
                System.out.format("AkkaRear.LocalMediator <serving> got Batch from %s%n", context().sender());
                lastBatch = b;
                System.out.format("AkkaRear.LocalMediator <serving> %s got Batch with payload %s%n", clazz, b.payload(clazz));
                b.payload(clazz).forEach(e -> {
                  System.out.format("AkkaRear.LocalMediator <serving> got Batch with payload item %s %s%n", e, e.getClass());
                  consumer.accept(e);
                } );
                sender().tell(new BatchAccepted(), self());
              })
              .match(GimmeLastBatch.class, g -> {
                System.out.format("AkkaRear.LocalMediator <serving> got GimmeLastBatch %s from 5s%n", g, context().sender());
                sender().tell(lastBatch, self());
              })
              .build();
    }
  }

  public static class Handle<T> {
    private final ActorRef localMediator;

    Handle(ActorRef localMediator) {
      this.localMediator = localMediator;
    }

    public void addListener(Consumer<T> sink) {
      localMediator.tell(sink, ActorRef.noSender());
    }

    @Override
    public String toString() { return String.format("Handle localMediator %s", localMediator); }
  }
}
