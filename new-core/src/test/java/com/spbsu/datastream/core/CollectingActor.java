package com.spbsu.datastream.core;

import akka.actor.Props;
import com.spbsu.datastream.core.front.RawData;

import java.util.Queue;
import java.util.function.Consumer;

final class CollectingActor<T> extends LoggingActor {
  private final Consumer<T> queue;

  public static <T> Props props(Consumer<T> queue) {
    return Props.create(CollectingActor.class, queue);
  }

  private CollectingActor(Consumer<T> queue) {
    this.queue = queue;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(RawData.class, m -> this.queue.accept((T) m.payload())).build();
  }
}
