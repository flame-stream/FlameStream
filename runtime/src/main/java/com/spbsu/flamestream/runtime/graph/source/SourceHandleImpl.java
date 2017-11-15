package com.spbsu.flamestream.runtime.graph.source;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.source.SourceHandle;
import com.spbsu.flamestream.runtime.graph.AtomicHandleImpl;
import com.spbsu.flamestream.runtime.graph.source.api.Accepted;
import com.spbsu.flamestream.runtime.graph.source.api.Heartbeat;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.spbsu.flamestream.runtime.tick.TickRoutes;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Artem
 * Date: 10.11.2017
 */
public class SourceHandleImpl extends AtomicHandleImpl implements SourceHandle {
  private final Map<String, ActorRef> frontRefs = new HashMap<>();

  SourceHandleImpl(TickInfo tickInfo, TickRoutes tickRoutes, ActorContext context) {
    super(tickInfo, tickRoutes, context);
  }

  @Override
  public void heartbeat(GlobalTime time) {
    tickRoutes.acker().tell(new Heartbeat(time), context.self());
  }

  @Override
  public void accept(GlobalTime globalTime) {
    frontRefs.get(globalTime.front()).tell(new Accepted(globalTime), context.self());
  }

  void putRef(String frontId, ActorRef frontRef) {
    frontRefs.putIfAbsent(frontId, frontRef);
  }
}
