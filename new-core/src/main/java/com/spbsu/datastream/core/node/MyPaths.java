package com.spbsu.datastream.core.node;

import akka.actor.ActorPath;
import akka.actor.Address;
import akka.actor.RootActorPath;

import java.net.InetSocketAddress;

public final class MyPaths {
  private MyPaths() {
  }

  public static ActorPath front(final int id, final InetSocketAddress address) {
    final Address add = Address.apply("akka.tcp", "worker", address.getAddress().getHostName(), address.getPort());
    return RootActorPath.apply(add, "/")
            .$div("user")
            .$div(String.valueOf(id))
            .$div("front");
  }
}
