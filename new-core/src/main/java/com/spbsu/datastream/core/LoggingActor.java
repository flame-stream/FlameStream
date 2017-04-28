package com.spbsu.datastream.core;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.Option;

public abstract class LoggingActor extends UntypedActor {
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
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    this.LOG().error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void unhandled(final Object message) {
    this.LOG().error("Can't handle message: {}", message);
    super.unhandled(message);
  }

  protected final LoggingAdapter LOG() {
    return this.LOG;
  }
}
