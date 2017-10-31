package com.spbsu.flamestream.example;

import com.spbsu.flamestream.runtime.TestEnvironment;
import com.spbsu.flamestream.runtime.environment.local.LocalClusterEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public abstract class AbstractExampleTest {
  protected abstract FlameStreamExample example();

  protected <T> void test(ExampleChecker<T> checker, int fronts, int workers, int tickLengthInSec, int waitTickInSec) {
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(workers);
         TestEnvironment environment = new TestEnvironment(lce)) {
      final List<Object> result = new ArrayList<>();
      //noinspection RedundantCast,unchecked
      environment.deploy(environment.withFusedFronts(example().graph(h -> environment.wrapInSink(
        (ToIntFunction<? super T>) h,
        result::add
      ))), tickLengthInSec, 1);

      final Consumer<Object> sink = environment.randomFrontConsumer(fronts);
      checker.input().forEach(wikipediaPage -> {
        sink.accept(wikipediaPage);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      environment.awaitTick(waitTickInSec);
      //noinspection unchecked
      checker.assertCorrect(result.stream());
    }
  }
}
