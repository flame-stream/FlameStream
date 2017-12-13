package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;

import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 12.12.2017
 */
public class SinkJoba extends Joba.Stub {
  private final BiConsumer<DataItem, ActorRef> barrier;

  public SinkJoba(BiConsumer<DataItem, ActorRef> barrier, ActorRef acker, ActorContext context) {
    super(Stream.empty(), acker, context);
    this.barrier = barrier;
  }

  @Override
  public boolean isAsync() {
    return true;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    barrier.accept(dataItem, context.self());
    process(dataItem, Stream.of(dataItem), fromAsync);
  }
}
