package com.spbsu.datastream.core.front;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.LoggingActor;

public final class FrontActor extends LoggingActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  private final ActorRef remoteRouter;

  private final String id;

  public static Props props(final ActorRef remoteRouter, final String id) {
    return Props.create(FrontActor.class, remoteRouter, id);
  }

  private FrontActor(final ActorRef remoteRouter, final String id) {
    this.remoteRouter = remoteRouter;
    this.id = id;
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
  }
}
