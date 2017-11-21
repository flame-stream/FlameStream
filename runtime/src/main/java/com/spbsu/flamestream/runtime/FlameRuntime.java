package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Graph;

import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface FlameRuntime {
  Flame run(Graph g);

  interface Flame {
    void stop();

    <T extends Front> Stream<FrontHandle<T>> attachFront(Class<T> front, Properties props);
    <T extends Rear> Stream<RearHandle<T>> outputOf(Class<T> rear, Properties props);
  }

  interface FrontHandle<T> {
  }

  interface RearHandle<T> {
  }

  interface Front {
    void onStart(Consumer<Object> handle);
    void onRequest(GlobalTime from);
    void onCheckpoint(GlobalTime from);
  }

  interface Rear extends Consumer<Object>{
  }
}
