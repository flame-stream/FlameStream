package com.spbsu.flamestream.runtime.environment;

import akka.actor.Props;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.tick.TickInfo;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

public interface Environment extends AutoCloseable {
  void deploy(TickInfo tickInfo);

  void deployFront(String nodeId, String frontId, Props frontProps);

  Set<String> availableWorkers();

  <T> AtomicGraph wrapInSink(ToIntFunction<? super T> hash, Consumer<? super T> mySuperConsumer);

  void awaitTick(long tickId) throws InterruptedException;

  Set<Long> ticks();
}
