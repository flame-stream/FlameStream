package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;

import java.util.List;
import java.util.stream.Stream;

public interface FlameRuntime {
  Flame run(Graph g);

  interface Flame {
    void extinguish();

    <T extends Front, H extends FrontHandle> Stream<H> attachFront(Class<T> front, List<Object> props);

    <T extends Rear, H extends RearHandle> Stream<H> outputOf(Class<T> rear, List<Object> props);
  }

  interface FrontHandle {
  }

  interface RearHandle {

  }
}
