package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;

import java.util.stream.Stream;

public interface FlameRuntime {
  Flame run(Graph g);

  interface Flame {
    void extinguish();

    <T extends Front, H extends FrontHandle> Stream<H> attachFront(String name, Class<T> front, String... args);

    <T extends Rear, H extends RearHandle> Stream<H> attachRear(String name, Class<T> rear, String... args);
  }

  interface FrontHandle {
  }

  interface RearHandle {
  }
}
