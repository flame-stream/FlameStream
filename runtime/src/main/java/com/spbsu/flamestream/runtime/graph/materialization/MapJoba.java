package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapJoba extends Joba.Stub {
  private final FlameMap<?, ?> flameMap;
  private int localTime = 0;

  public MapJoba(FlameMap<?, ?> flameMap, Stream<Joba> outJobas, ActorRef acker, ActorContext context) {
    super(outJobas, acker, context);
    this.flameMap = flameMap;
  }

  @Override
  public boolean isAsync() {
    return false;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    final Stream<DataItem> output = flameMap.operation().apply(dataItem, localTime++);
    process(dataItem, output, fromAsync);
  }
}
