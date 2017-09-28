package com.spbsu.flamestream.example;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.TestEnvironment;
import com.spbsu.flamestream.runtime.TheGraph;
import com.spbsu.flamestream.runtime.environment.local.LocalClusterEnvironment;

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
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(workers); TestEnvironment environment = new TestEnvironment(lce)) {
      final List<Object> result = new ArrayList<>();
      environment.deploy(graph(
              environment.availableFronts(),
              environment.wrapInSink(result::add)
      ), tickLengthInSec, 1);

      final Consumer<Object> sink = environment.randomFrontConsumer(fronts);
      checker.input().forEach(wikipediaPage -> {
        sink.accept(wikipediaPage);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException();
        }
      });

      environment.awaitTick(waitTickInSec);
      //noinspection unchecked
      checker.check(result.stream());
    }
  }
}
