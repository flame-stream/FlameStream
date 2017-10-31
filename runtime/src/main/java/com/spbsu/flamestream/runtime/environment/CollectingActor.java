package com.spbsu.flamestream.runtime.environment;

import akka.actor.Props;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.raw.RawData;

import java.util.function.Consumer;

public final class CollectingActor<T> extends LoggingActor {
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
    return receiveBuilder().match(RawData.class, m -> m.forEach(o -> consumer.accept((T) o))).build();
  }
}
