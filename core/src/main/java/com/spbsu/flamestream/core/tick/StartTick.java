package com.spbsu.flamestream.core.tick;

public final class StartTick {
  private final TickRoutes tickRoutes;

  public StartTick(TickRoutes tickRoutes) {
    this.tickRoutes = tickRoutes;
  }

  public TickRoutes tickRoutingInfo() {
    return tickRoutes;
  }

  @Override
  public String toString() {
    return "StartTick{" +
            "tickRoutes=" + tickRoutes +
            '}';
  }
}
