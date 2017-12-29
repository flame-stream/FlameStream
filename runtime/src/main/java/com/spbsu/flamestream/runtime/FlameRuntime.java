package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

import java.util.stream.Stream;

public interface FlameRuntime extends AutoCloseable {
  int DEFAULT_MAX_ELEMENTS_IN_GRAPH = 100;

  Flame run(Graph g);

  interface Flame extends AutoCloseable{
    <F extends Front, H> Stream<H> attachFront(String id, FrontType<F, H> type);

    <R extends Rear, H> Stream<H> attachRear(String id, RearType<R, H> type);
  }

  interface FrontType<F extends Front, H> {
    FrontInstance<F> instance();

    H handle(EdgeContext context);
  }

  interface RearType<R extends Rear, H> {
    RearInstance<R> instance();

    H handle(EdgeContext context);
  }

  interface FrontInstance<F extends Front> {
    Class<F> clazz();

    Object[] params();
  }

  interface RearInstance<R extends Rear> {
    Class<R> clazz();

    Object[] params();
  }
}
