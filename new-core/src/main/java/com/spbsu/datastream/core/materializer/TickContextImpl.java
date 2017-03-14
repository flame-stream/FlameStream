package com.spbsu.datastream.core.materializer;

import akka.actor.ActorRef;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;

import java.util.Map;

public class TickContextImpl implements TickContext {
  private final Map<OutPort, InPort> downstreams;

  private final ActorRef forkRouter;

  public TickContextImpl(final Map<OutPort, InPort> downstreams,
                         final ActorRef forkRouter) {
    this.downstreams = downstreams;
    this.forkRouter = forkRouter;
  }

  @Override
  public Map<OutPort, InPort> downstreams() {
    return null;
  }

  @Override
  public ActorRef forkRouter() {
    return null;
  }
}
