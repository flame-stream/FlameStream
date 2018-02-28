package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.function.Consumer;

public class AkkaRearType<T> implements FlameRuntime.RearType<AkkaRear, AkkaRearType<T>.Handle> {
  private final ActorSystem system;
  private final Class<T> clazz;

  public AkkaRearType(ActorSystem system, Class<T> clazz) {
    this.system = system;
    this.clazz = clazz;
  }

  @Override
  public FlameRuntime.RearInstance<AkkaRear> instance() {
    return new FlameRuntime.RearInstance<AkkaRear>() {
      @Override
      public Class<AkkaRear> clazz() {
        return AkkaRear.class;
      }

      @Override
      public String[] params() {
        return new String[0];
      }
    };
  }

  @Override
  public Handle handle(EdgeContext context) {
    return new Handle(context);
  }

  public class Handle {
    private final ActorRef rear;

    Handle(EdgeContext context) {
      this.rear = AwaitResolver.syncResolve(context.nodePath()
              .child("edge")
              .child(context.edgeId().edgeName())
              .child(context.edgeId().nodeId() + "-inner"), system);
    }

    public void addListener(Consumer<T> consumer) {
      final ActorRef rearConsumer = system.actorOf(InnerActor.props(consumer, clazz));
      rear.tell(rearConsumer, ActorRef.noSender());
    }
  }

  private static class InnerActor<T> extends LoggingActor {
    private final Consumer<T> consumer;
    private final Class<T> clazz;

    private InnerActor(Consumer<T> consumer, Class<T> clazz) {
      this.consumer = consumer;
      this.clazz = clazz;
    }

    static <T> Props props(Consumer<T> consumer, Class<T> clazz) {
      return Props.create(InnerActor.class, consumer, clazz);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(DataItem.class, dataItem -> consumer.accept(dataItem.payload(clazz)))
              .build();
    }
  }
}
