package com.spbsu.datastream.core;

import akka.actor.Props;

import java.util.Queue;

final class CollectorActor<T> extends LoggingActor {
  private final Queue<T> queue;

  public static <T> Props props(final Queue<T> queue) {
    return Props.create(CollectorActor.class, queue);
  }

  private CollectorActor(final Queue<T> queue) {
    this.queue = queue;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onReceive(final Object message) throws Throwable {
    this.queue.offer((T) message);
  }
}
