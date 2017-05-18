package com.spbsu.datastream.core;

import akka.actor.Props;
import com.spbsu.datastream.core.front.RawData;

import java.util.Queue;

final class CollectorActor<T> extends LoggingActor {
  private final Queue<T> queue;

  public static <T> Props props(Queue<T> queue) {
    return Props.create(CollectorActor.class, queue);
  }

  private CollectorActor(Queue<T> queue) {
    this.queue = queue;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(RawData.class, m -> this.queue.offer((T) m.payload())).build();
  }
}
