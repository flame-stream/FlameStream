package com.spbsu.datastream.core;

import akka.actor.AbstractActor;
import akka.actor.SupervisorStrategy;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.Optional;

public abstract class LoggingActor extends AbstractActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  @Override
  public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
    receive.apply(msg);
    //final Message message;
    //
    //if (msg instanceof UnresolvedMessage) {
    //  message = ((UnresolvedMessage) msg).payload();
    //} else if (msg instanceof Message) {
    //  message = ((Message) msg);
    //} else {
    //  receive.apply(msg);
    //  return;
    //}
    //
    //final AtomicMessage atomicMessage;
    //
    //if (message instanceof AtomicMessage) {
    //  atomicMessage = ((AtomicMessage) message);
    //} else {
    //  receive.apply(msg);
    //  return;
    //}
    //
    //final Object data = atomicMessage.payload().payload();
    //
    //if (data.toString().contains("фигей")) {
    //  LOG.info("Before: {}", System.nanoTime());
    //  receive.apply(msg);
    //  LOG.info("After: {}", System.nanoTime());
    //} else {
    //  receive.apply(msg);
    //}
  }

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

  @Override
  public SupervisorStrategy supervisorStrategy() {
    return SupervisorStrategy.stoppingStrategy();
  }

  protected final LoggingAdapter LOG() {
    return this.LOG;
  }
}
