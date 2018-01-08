package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapJoba extends Joba.Stub {
  private final FlameMap<?, ?>.FlameMapOperation operation;

  public MapJoba(FlameMap<?, ?> flameMap, Stream<Joba> outJobas, ActorRef acker, ActorContext context) {
    super(outJobas, acker, context);
    this.operation = flameMap.operation(ThreadLocalRandom.current().nextLong());
  }

  @Override
  public boolean isAsync() {
    return false;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    final Stream<DataItem> output = operation.apply(dataItem);
    processWithBuffer(dataItem, output, fromAsync);
  }
}
