package com.spbsu.datastream.core.routing;

import akka.actor.ActorRef;

public interface RootRouterApi {
  class RegisterMe {
    private final int tick;
    private final ActorRef actorRef;

    public RegisterMe(final int tick, final ActorRef actorRef) {
      this.tick = tick;
      this.actorRef = actorRef;
    }

    public int tick() {
      return tick;
    }

    public ActorRef actorRef() {
      return actorRef;
    }

    @Override
    public String toString() {
      return "RegisterMe{" + "tick=" + tick +
              ", actorRef=" + actorRef +
              '}';
    }
  }
}
