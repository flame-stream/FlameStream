package com.spbsu.flamestream.runtime.environment;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.environment.raw.RawData;

import java.util.function.Consumer;

public class CollectingActor<T> extends LoggingActor {
  private final Consumer<T> consumer;

  private CollectingActor(Consumer<T> consumer) {
    this.consumer = consumer;
  }

  public static <T> Props props(Consumer<T> queue) {
    return Props.create(CollectingActor.class, queue);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(RawData.class, m -> m.forEach(o -> consumer.accept((T) o)))
            .build();
  }
}
