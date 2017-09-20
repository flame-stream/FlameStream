package com.spbsu.datastream.core.tick;

public final class StartTick {
  private final RoutingInfo routingInfo;

  public StartTick(RoutingInfo routingInfo) {
    this.routingInfo = routingInfo;
  }

  public RoutingInfo tickRoutingInfo() {
    return routingInfo;
  }

  @Override
  public String toString() {
    return "StartTick{" +
            "routingInfo=" + routingInfo +
            '}';
  }
}
