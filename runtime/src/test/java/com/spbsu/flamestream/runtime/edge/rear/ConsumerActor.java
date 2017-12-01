package com.spbsu.flamestream.runtime.edge.rear;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.function.Consumer;

public class ConsumerActor<T> extends LoggingActor {
  private final Consumer<T> consumer;

  private ConsumerActor(Consumer<T> consumer) {
    this.consumer = consumer;
  }

  public static <T> Props props(Consumer<T> consumer) {
    return Props.create(ConsumerActor.class, consumer);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .matchAny(data -> consumer.accept((T) data))
            .build();
  }
}
