package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.node.TickInfo;

public final class TickContextImpl implements TickContext {
  private final ActorRef rootRouter;
  private final ActorRef rangeRouter;

  private final HashRange localRange;
  private final TickInfo tickInfo;

  public TickContextImpl(final ActorRef rootRouter,
                         final ActorRef rangeRouter,
                         final HashRange localRange,
                         final TickInfo tickInfo) {
    this.rangeRouter = rangeRouter;
    this.rootRouter = rootRouter;
    this.localRange = localRange;
    this.tickInfo = tickInfo;
  }

  @Override
  public TickInfo tickInfo() {
    return this.tickInfo;
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
