package com.spbsu.benchamrk.flink.streams;

import com.spbsu.benchmark.flink.streams.inverted_index.InvertedIndexFold;
import com.spbsu.benchmark.flink.streams.inverted_index.InvertedIndexStream;
import com.spbsu.flamestream.example.ExampleChecker;
import com.spbsu.flamestream.example.inverted_index.InvertedIndexCheckers;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
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
public class InvertedIndexStreamTest {
  //dirty code for avoiding serialization
  private static Iterator<WikipediaPage> sourceIterator;

  private static FlinkLocalExecutor executor;

  @BeforeClass
  public void setUpClass() {
    executor = new FlinkLocalExecutor(1);
  }

  @DataProvider(name = "correctnessProvider")
  public static Object[][] primeNumbers() {
    return new Object[][]{
            {InvertedIndexCheckers.CHECK_INDEX_WITH_SMALL_DUMP},
            {InvertedIndexCheckers.CHECK_INDEX_AND_RANKING_STORAGE_WITH_SMALL_DUMP},
            {InvertedIndexCheckers.CHECK_INDEX_WITH_RANKING}
    };
  }

  @Test(dataProvider = "correctnessProvider")
  public void testCorrectness(ExampleChecker<WikipediaPage> checker) {
    sourceIterator = checker.input().iterator();

    final Collection<Object> output = new ArrayList<>();
    executor.execute(new InvertedIndexStream(), new Source(), o -> {
      final InvertedIndexFold fold = (InvertedIndexFold) o;
      output.add(fold.wordIndexAdd());
      if (fold.wordIndexRemove() != null)
        output.add(fold.wordIndexRemove());
    });
    checker.assertCorrect(output.stream());
  }

  private static class Source implements SourceFunction<WikipediaPage> {
    private boolean running = true;

    @Override
    public void run(SourceContext<WikipediaPage> ctx) throws Exception {
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
