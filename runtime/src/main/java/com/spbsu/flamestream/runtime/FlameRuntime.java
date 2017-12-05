package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.SystemEdgeContext;

import java.util.stream.Stream;

public interface FlameRuntime {
  Flame run(Graph g);

  interface Flame {
    void extinguish();

    <F extends Front, H> Stream<H> attachFront(String id, FrontType<F, H> type, String... args);

    <R extends Rear, H> Stream<H> attachRear(String id, RearType<R, H> type, String... args);
  }

  interface FrontType<F extends Front, H> {
    Class<F> frontClass();

    H handle(EdgeContext context);
  }

  interface RearType<R extends Rear, H> {
    Class<R> rearClass();

    H handle(EdgeContext context);
  }
}
