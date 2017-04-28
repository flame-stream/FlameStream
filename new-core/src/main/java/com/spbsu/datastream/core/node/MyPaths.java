package com.spbsu.datastream.core.node;

import akka.actor.ActorPath;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.datastream.core.configuration.HashRange;

import java.net.InetSocketAddress;

public final class MyPaths {
  private MyPaths() {
  }

  public static ActorPath rangeConcierge(final InetSocketAddress address, final HashRange range) {
    final Address add = Address.apply("akka.tcp", "worker", address.getAddress().getHostName(), address.getPort());
    return RootActorPath.apply(add, "/")
            .$div("user")
            .$div("root")
            .$div(range.toString());
  }

  public static ActorPath rangeRouter(final InetSocketAddress address, final HashRange range) {
    final Address add = Address.apply("akka.tcp", "worker", address.getAddress().getHostName(), address.getPort());
    return RootActorPath.apply(add, "/")
            .$div("user")
            .$div("root")
            .$div(range.toString())
            .$div("rangeRouter");
  }
}
