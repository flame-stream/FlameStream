package com.spbsu.flamestream.runtime.source;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.TickInfo;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.source.SourceHandle;
import com.spbsu.flamestream.runtime.range.atomic.AtomicHandleImpl;
import com.spbsu.flamestream.runtime.source.api.Accepted;
import com.spbsu.flamestream.runtime.source.api.Heartbeat;
import com.spbsu.flamestream.runtime.tick.TickRoutes;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Artem
 * Date: 10.11.2017
 */
public class SourceHandleImpl extends AtomicHandleImpl implements SourceHandle {
  private final Map<Integer, ActorRef> frontRefs = new HashMap<>();

  SourceHandleImpl(TickInfo tickInfo, TickRoutes tickRoutes, ActorContext context) {
    super(tickInfo, tickRoutes, context);
  }

  @Override
  public void heartbeat(GlobalTime time) {
    tickRoutes.acker().tell(new Heartbeat(time), context.self());
  }

  @Override
  public void accept(DataItem<?> dataItem) {
    frontRefs.get(dataItem.meta().globalTime().front()).tell(new Accepted(dataItem), context.self());
  }

  void putRef(int frontId, ActorRef frontRef) {
    frontRefs.putIfAbsent(frontId, frontRef);
  }
}
