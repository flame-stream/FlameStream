package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

import java.util.Collections;
import java.util.Set;

public final class TickContextImpl implements TickContext {
  private final ActorRef rootRouter;
  private final ActorRef rangeRouter;

  private final HashRange localRange;
  private final HashRange ackerRange;
  private final TheGraph graph;

  private final long tick;
  private final long window;

  public TickContextImpl(final ActorRef rootRouter,
                         final ActorRef rangeRouter,
                         final TheGraph graph,
                         final long tick,
                         final long window,
                         final HashRange localRange,
                         final HashRange ackerRange) {
    this.rangeRouter = rangeRouter;
    this.rootRouter = rootRouter;
    this.tick = tick;
    this.localRange = localRange;
    this.window = window;
    this.graph = graph;
    this.ackerRange = ackerRange;
  }

  @Override
  public TheGraph graph() {
    return this.graph;
  }

  @Override
  public long tick() {
    return this.tick;
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
  public HashRange ackerRange() {
    return this.ackerRange;
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
