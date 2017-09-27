package com.spbsu.flamestream.runtime.environment;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.tick.TickInfo;

import java.util.Set;
import java.util.function.Consumer;

public interface Environment extends AutoCloseable {
  void deploy(TickInfo tickInfo);

  Set<Integer> availableFronts();

  Set<Integer> availableWorkers();

  <T> AtomicGraph wrapInSink(Consumer<T> mySuperConsumer);

  Consumer<Object> frontConsumer(int frontId);
}
