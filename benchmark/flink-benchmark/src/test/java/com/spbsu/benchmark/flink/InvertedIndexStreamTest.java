package com.spbsu.benchmark.flink;

import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.example.ExampleChecker;
import com.spbsu.flamestream.example.inverted_index.InvertedIndexCheckers;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.utils.IndexItemInLong;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class InvertedIndexStreamTest {
  private final Logger LOG = LoggerFactory.getLogger(InvertedIndexStreamTest.class);
  //dirty code for avoiding serialization
  private static Iterator<WikipediaPage> sourceIterator;

  private static FlinkLocalExecutor executor;

  @BeforeClass
  public void setUpClass() {
    executor = new FlinkLocalExecutor(0);
  }

  @DataProvider(name = "correctnessProvider")
  public static Object[][] correctnessProvider() {
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
      final InvertedIndexStream.Result out = (InvertedIndexStream.Result) o;
      output.add(out.wordIndexAdd());
      if (out.wordIndexRemove() != null)
        output.add(out.wordIndexRemove());
    });

    checker.assertCorrect(output.stream());
  }

  @DataProvider(name = "measureProvider")
  public static Object[][] measureProvider() {
    return new Object[][]{
            {InvertedIndexCheckers.CHECK_INDEX_WITH_RANKING, 10}
    };
  }

  @Test(dataProvider = "measureProvider")
  public void measureLatency(ExampleChecker<WikipediaPage> checker, int warmUpDelay) {
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(warmUpDelay, 0);
    sourceIterator = checker.input().peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id())).iterator();

    executor.execute(new InvertedIndexStream(), new Source(), o -> {
      final InvertedIndexStream.Result out = (InvertedIndexStream.Result) o;
      final WordIndexAdd wordIndexAdd = out.wordIndexAdd();
      final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);
      latencyMeasurer.finish(docId);
    });

    final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
    LOG.warn("Latencies stat: {}", stat);
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
