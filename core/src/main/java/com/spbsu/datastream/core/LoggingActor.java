package com.spbsu.datastream.core;

import akka.actor.AbstractActor;
import akka.actor.SupervisorStrategy;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.Optional;

public abstract class LoggingActor extends AbstractActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  @Override
  public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
    receive.apply(msg);
  }

  @Override
  public void preStart() throws Exception {
    LOG().info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    LOG().info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
    LOG().error("Restarting, reason: {}, payload: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void unhandled(Object message) {
    LOG().error("Can't handle payload: {}", message);
    super.unhandled(message);
  }

  @Override
  public SupervisorStrategy supervisorStrategy() {
    final SupervisorStrategy supervisorStrategy = super.supervisorStrategy();
    return SupervisorStrategy.stoppingStrategy();
  }

  protected final LoggingAdapter LOG() {
    return LOG;
  }
}
