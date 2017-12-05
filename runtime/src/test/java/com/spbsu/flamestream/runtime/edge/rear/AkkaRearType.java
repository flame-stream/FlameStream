package com.spbsu.flamestream.runtime.edge.rear;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.RawData;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.function.Consumer;

public class AkkaRearType implements FlameRuntime.RearType<AkkaRear, AkkaRearType.Handle> {
  private final ActorSystem system;

  public AkkaRearType(ActorSystem system) {
    this.system = system;
  }

  @Override
  public Class<AkkaRear> rearClass() {
    return AkkaRear.class;
  }

  @Override
  public Handle handle(EdgeContext context) {
    return new Handle(context);
  }

  public class Handle {
    private final ActorRef rear;

    public Handle(EdgeContext context) {
      this.rear = AwaitResolver.syncResolve(context.nodePath()
              .child("edge")
              .child(context.edgeId() + "-inner"), system);
    }

    public void addListener(Consumer<Object> consumer) {
      final ActorRef rearConsumer = system.actorOf(InnerActor.props(consumer));
      rear.tell(rearConsumer, ActorRef.noSender());
    }
  }

  private static class InnerActor extends LoggingActor {
    private final Consumer<Object> consumer;

    private InnerActor(Consumer<Object> consumer) {
      this.consumer = consumer;
    }

    public static Props props(Consumer<Object> consumer) {
      return Props.create(InnerActor.class, consumer);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(RawData.class, d -> consumer.accept(d.data()))
              .build();
    }
  }
}
