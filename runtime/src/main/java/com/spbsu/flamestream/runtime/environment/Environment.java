package com.spbsu.flamestream.runtime.environment;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.tick.TickInfo;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

public interface Environment extends AutoCloseable {
  void deploy(TickInfo tickInfo);

  Set<Integer> availableFronts();

  Set<Integer> availableWorkers();

  <T> AtomicGraph wrapInSink(ToIntFunction<? super T> hash, Consumer<? super T> mySuperConsumer);

  Consumer<Object> frontConsumer(int frontId);
}
