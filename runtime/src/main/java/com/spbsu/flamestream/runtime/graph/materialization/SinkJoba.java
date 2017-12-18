package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.DataItem;

import java.util.List;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 12.12.2017
 */
public class SinkJoba extends Joba.Stub {
  private final List<ActorRef> barriers;

  public SinkJoba(List<ActorRef> barriers, ActorRef acker, ActorContext context) {
    super(Stream.empty(), acker, context);
    this.barriers = barriers;
  }

  @Override
  public boolean isAsync() {
    return true;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    final int hash = Hashing.murmur3_32().hashLong(dataItem.meta().globalTime().time()).asInt();
    barriers.get(Math.abs(hash) % barriers.size()).tell(dataItem, context.self());
    process(dataItem, Stream.of(dataItem), fromAsync);
  }
}
