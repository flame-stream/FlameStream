package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.barrier.BarrierCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 12.12.2017
 */
public class SinkJoba extends Joba.Stub implements MinTimeHandler {
  private final BarrierCollector collector = new BarrierCollector();
  private final List<ActorRef> rears = new ArrayList<>();

  public SinkJoba(ActorRef acker, ActorContext context) {
    super(Stream.empty(), acker, context);
  }

  public void addRear(ActorRef rear) {
    rears.add(rear);
  }

  @Override
  public boolean isAsync() {
    return false;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    collector.enqueue(dataItem);
    process(dataItem, Stream.of(dataItem), fromAsync);
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    collector.releaseFrom(minTime, di -> rears.forEach(rear -> rear.tell(di, context.self())));
  }
}
