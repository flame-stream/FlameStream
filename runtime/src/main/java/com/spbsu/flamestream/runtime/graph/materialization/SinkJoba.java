package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;

import java.util.Map;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 12.12.2017
 */
public class SinkJoba extends Joba.Stub {
  private final Map<String, ActorRef> barriers;

  public SinkJoba(Map<String, ActorRef> barriers, ActorRef acker, ActorContext context) {
    super(Stream.empty(), acker, context);
    this.barriers = barriers;
  }

  @Override
  public boolean isAsync() {
    return true;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    barriers.get(dataItem.meta().globalTime().frontId().nodeId()).tell(dataItem, context.self());
    process(dataItem, Stream.of(dataItem), fromAsync);
  }
}
