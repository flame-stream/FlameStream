package com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.source.SourceHandle;
import com.spbsu.flamestream.runtime.node.materializer.GraphRoutes;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.AtomicHandleImpl;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.api.Accepted;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.api.Heartbeat;

import java.util.HashMap;
import java.util.Map;

class SourceHandleImpl extends AtomicHandleImpl implements SourceHandle {
  private final Map<String, ActorRef> frontRefs = new HashMap<>();

  SourceHandleImpl(GraphRoutes routes, ActorContext context) {
    super(routes, context);
  }

  @Override
  public void heartbeat(GlobalTime time) {
    routes.acker().tell(new Heartbeat(time), context.self());
  }

  @Override
  public void accept(GlobalTime globalTime) {
    frontRefs.get(globalTime.front()).tell(new Accepted(globalTime), context.self());
  }

  void putRef(String frontId, ActorRef frontRef) {
    frontRefs.putIfAbsent(frontId, frontRef);
  }
}
