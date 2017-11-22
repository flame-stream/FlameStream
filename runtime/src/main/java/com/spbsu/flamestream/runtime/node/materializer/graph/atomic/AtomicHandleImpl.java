package com.spbsu.flamestream.runtime.node.materializer.graph.atomic;

import akka.actor.ActorContext;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.utils.Statistics;
import com.spbsu.flamestream.runtime.node.materializer.GraphRoutes;
import com.spbsu.flamestream.runtime.node.materializer.acker.api.Ack;

public class AtomicHandleImpl implements AtomicHandle {
  protected final ActorContext context;
  protected final GraphRoutes routes;

  private final LoggingAdapter log;

  public AtomicHandleImpl(GraphRoutes graphRoutes, ActorContext context) {
    this.routes = graphRoutes;
    this.context = context;
    log = Logging.getLogger(context.system(), context.self());
  }

  @Override
  public void push(OutPort out, DataItem<?> result) {
    routes.router().tell(result, out, context.self());
  }

  @Override
  public void ack(long xor, GlobalTime globalTime) {
    final Ack message = new Ack(globalTime, xor);
    routes.acker().tell(message, context.self());
  }

  @Override
  public void submitStatistics(Statistics stat) {
    log.info("Inner statistics: {}", stat);
  }

  @Override
  public void error(String format, Object... args) {
    log.warning(format, args);
  }

  public ActorContext backdoor() {
    return context;
  }
}
