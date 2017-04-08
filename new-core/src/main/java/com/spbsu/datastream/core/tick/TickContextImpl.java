package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

import java.util.concurrent.atomic.AtomicInteger;

public final class TickContextImpl implements TickContext {
  private final ActorRef rootRouter;

  private final long tick;

  private final HashRange localRange;

  private final TheGraph graph;

  private final AtomicInteger localTime;

  public TickContextImpl(final ActorRef rootRouter,
                         final long tick,
                         final HashRange localRange,
                         final TheGraph graph) {
    this.rootRouter = rootRouter;
    this.tick = tick;
    this.localRange = localRange;
    this.graph = graph;
    this.localTime = new AtomicInteger();
  }

  @Override
  public TheGraph graph() {
    return graph;
  }

  @Override
  public long tick() {
    return tick;
  }

  @Override
  public HashRange localRange() {
    return localRange;
  }

  @Override
  public int incrementLocalTimeAndGet() {
    return localTime.incrementAndGet();
  }

  @Override
  public ActorRef rootRouter() {
    return rootRouter;
  }
}
