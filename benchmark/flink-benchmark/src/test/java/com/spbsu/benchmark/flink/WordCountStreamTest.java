package com.spbsu.benchmark.flink;

import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.example.ExampleChecker;
import com.spbsu.flamestream.example.wordcount.WordCountCheckers;
import com.spbsu.flamestream.example.wordcount.model.WordCounter;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toMap;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class WordCountStreamTest {
  private final Logger LOG = LoggerFactory.getLogger(InvertedIndexStreamTest.class);
  //dirty code for avoiding serialization
  private static Iterator<String> sourceIterator;

  private static FlinkLocalExecutor executor;

  @BeforeClass
  public void setUpClass() {
    executor = new FlinkLocalExecutor(0);
  }

  @DataProvider(name = "correctnessProvider")
  public static Object[][] correctnessProvider() {
    return new Object[][]{
            {WordCountCheckers.CHECK_COUNT}
    };
  }

  @Test(dataProvider = "correctnessProvider")
  public void testCorrectnessAndMeasureLatency(ExampleChecker<String> checker) {
    sourceIterator = checker.input().iterator();

    final Collection<Object> output = new ArrayList<>();
    executor.execute(new WordCountStream(), new Source(), output::add);

    checker.assertCorrect(output.stream());
  }

  @DataProvider(name = "measureProvider")
  public static Object[][] measureProvider() {
    return new Object[][]{
            {WordCountCheckers.CHECK_COUNT, 0}
    };
  }

  @Test(dataProvider = "measureProvider")
  public void measureLatency(ExampleChecker<String> checker, int warmUpDelay) {
    final TObjectIntMap<String> expected = new TObjectIntHashMap<>();
    final LatencyMeasurer<WordCounter> latencyMeasurer = new LatencyMeasurer<>(warmUpDelay, 0);
    final Pattern pattern = Pattern.compile("\\s");

    sourceIterator = checker.input().peek(
            text -> Arrays.stream(pattern.split(text))
                    .collect(toMap(Function.identity(), o -> 1, Integer::sum))
                    .forEach((k, v) -> {
                      expected.adjustOrPutValue(k, v, v);
                      latencyMeasurer.start(new WordCounter(k, expected.get(k)));
                    })
    ).iterator();
    executor.execute(new WordCountStream(), new Source(), o -> latencyMeasurer.finish((WordCounter) o));

    final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
    LOG.warn("Latencies stat: {}", stat);
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
