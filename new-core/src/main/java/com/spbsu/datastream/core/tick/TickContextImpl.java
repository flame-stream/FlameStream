package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

public final class TickContextImpl implements TickContext {
  private final ActorRef rootRouter;
  private final ActorRef rangeRouter;

  private final HashRange localRange;

  private final TheGraph graph;

  private final int tick;
  private final long startTime;
  private final long window;

  public TickContextImpl(final ActorRef rootRouter,
                         final ActorRef rangeRouter,
                         final int tick,
                         final HashRange localRange,
                         final long startTime,
                         final long window,
                         final TheGraph graph) {
    this.rangeRouter = rangeRouter;
    this.rootRouter = rootRouter;
    this.tick = tick;
    this.localRange = localRange;
    this.startTime = startTime;
    this.window = window;
    this.graph = graph;
  }

  @Override
  public TheGraph graph() {
    return this.graph;
  }

  @Override
  public int tick() {
    return this.tick;
  }

  @Override
  public long startTime() {
    return this.startTime;
  }

  @Override
  public long window() {
    return this.window;
  }

  @Override
  public HashRange localRange() {
    return this.localRange;
  }

  @Override
  public ActorRef rootRouter() {
    return this.rootRouter;
  }

  @Override
  public ActorRef rangeRouter() {
    return this.rangeRouter;
  }
}
