package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;

import java.util.Properties;
import java.util.stream.Stream;

public interface FlameRuntime {
  Flame run(Graph g);

  interface Flame {
    void extinguish();

    <T extends Front<H>, H extends Front.Handle> Stream<H> attachFront(Class<T> front, Properties props);

    <T extends Rear<H>, H extends Rear.Handle> Stream<H> outputOf(Class<T> rear, Properties props);
  }
}
