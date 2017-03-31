package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;

public interface RootRouterApi {
  class RegisterMe {
    private final long tick;

    private final ActorRef actorRef;

    public RegisterMe(final long tick, final ActorRef actorRef) {
      this.tick = tick;
      this.actorRef = actorRef;
    }

    public long tick() {
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
