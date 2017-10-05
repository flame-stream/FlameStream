package com.spbsu.benchmark.flink;

import com.spbsu.flamestream.example.ExampleChecker;
import com.spbsu.flamestream.example.wordcount.WordCountCheckers;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class WordCountStreamTest {
  //dirty code for avoiding serialization
  private static Iterator<String> sourceIterator;

  private static FlinkLocalExecutor executor;

  @BeforeClass
  public void setUpClass() {
    executor = new FlinkLocalExecutor(1);
  }

  @DataProvider(name = "correctnessProvider")
  public static Object[][] primeNumbers() {
    return new Object[][]{
            {WordCountCheckers.CHECK_COUNT}
    };
  }

  @Test(dataProvider = "correctnessProvider")
  public void testCorrectness(ExampleChecker<String> checker) {
    sourceIterator = checker.input().iterator();

    final Collection<Object> output = new ArrayList<>();
    executor.execute(new WordCountStream(), new Source(), output::add);
    checker.assertCorrect(output.stream());
  }

  private static class Source implements SourceFunction<String> {
    private boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
      //noinspection Duplicates
      while (running) {
        if (sourceIterator.hasNext()) {
          ctx.collect(sourceIterator.next());
          ctx.emitWatermark(new Watermark(System.nanoTime()));
        } else {
          running = false;
          ctx.close();
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }
}
