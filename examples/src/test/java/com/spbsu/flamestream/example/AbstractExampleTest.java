package com.spbsu.flamestream.example;

import com.spbsu.flamestream.common.FlameStreamSuite;
import com.spbsu.flamestream.runtime.TestEnvironment;
import com.spbsu.flamestream.runtime.environment.local.LocalClusterEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.ToIntFunction;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public abstract class AbstractExampleTest extends FlameStreamSuite {
  protected abstract FlameStreamExample example();

  protected <T> void test(ExampleChecker<T> checker, int workers, int tickLengthInSec) {
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(workers);
            TestEnvironment environment = new TestEnvironment(lce)) {
      final List<Object> result = new ArrayList<>();
      //noinspection RedundantCast,unchecked
      environment.deploy(example().graph(h -> environment.wrapInSink(
              (ToIntFunction<? super T>) h,
              result::add
      )), (Spliterator<Object>) checker.input().spliterator(), tickLengthInSec, 1);

      environment.awaitTicks();
      Thread.sleep(1000); //wait for results

      checker.assertCorrect(result.stream());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
