package com.spbsu.flamestream.example;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.AbstractTestStand;
import com.spbsu.flamestream.runtime.RemoteTestStand;
import com.spbsu.flamestream.runtime.TheGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public abstract class AbstractExampleTest {
  protected abstract TheGraph graph(Collection<Integer> fronts, AtomicGraph sink);

  protected <T> void test(ExampleChecker<T> checker, int fronts, int workers, int tickLengthInSec, int waitTickInSec) {
    try (AbstractTestStand stage = new RemoteTestStand(workers)) {
      final List<Object> result = new ArrayList<>();
      stage.deploy(graph(
              stage.environment().availableFronts(),
              stage.environment().wrapInSink(result::add)
      ), tickLengthInSec, 1);

      final Consumer<Object> sink = stage.randomFrontConsumer(fronts);
      checker.input().forEach(wikipediaPage -> {
        sink.accept(wikipediaPage);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException();
        }
      });

      stage.awaitTick(waitTickInSec);
      //noinspection unchecked
      checker.check(result.stream());
    }
  }
}
