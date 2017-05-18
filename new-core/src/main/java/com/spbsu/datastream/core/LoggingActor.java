package com.spbsu.datastream.core;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.Optional;

public abstract class LoggingActor extends AbstractActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  @Override
  public void preStart() throws Exception {
    this.LOG().info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    this.LOG().info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
    this.LOG().error("Restarting, reason: {}, payload: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void unhandled(Object message) {
    this.LOG().error("Can't handle payload: {}", message);
    super.unhandled(message);
  }

  protected final LoggingAdapter LOG() {
    return this.LOG;
  }
}
