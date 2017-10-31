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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toMap;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class WordCountStreamTest {
  private final Logger log = LoggerFactory.getLogger(WordCountStreamTest.class);
  //dirty code for avoiding serialization
  private static Iterator<String> SOURCE_ITERATOR = null;

  private static FlinkLocalExecutor EXECUTOR = null;

  @BeforeClass
  public void setUpClass() {
    EXECUTOR = new FlinkLocalExecutor(0);
  }

  @DataProvider(name = "correctnessProvider")
  public static Object[][] correctnessProvider() {
    return new Object[][]{
            {WordCountCheckers.CHECK_COUNT}
    };
  }

  @Test(dataProvider = "correctnessProvider")
  public void testCorrectnessAndMeasureLatency(ExampleChecker<String> checker) {
    SOURCE_ITERATOR = checker.input().iterator();

    final Collection<Object> output = new ArrayList<>();
    EXECUTOR.execute(new WordCountStream(), new Source(), output::add);

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

    SOURCE_ITERATOR = checker.input()
            .peek(text -> Arrays.stream(pattern.split(text))
                    .collect(toMap(Function.identity(), o -> 1, Integer::sum))
                    .forEach((k, v) -> {
                      expected.adjustOrPutValue(k, v, v);
                      latencyMeasurer.start(new WordCounter(k, expected.get(k)));
                    }))
            .iterator();
    EXECUTOR.execute(new WordCountStream(), new Source(), o -> latencyMeasurer.finish((WordCounter) o));

    final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
    log.warn("Latencies stat: {}", stat);
  }

  private static class Source implements SourceFunction<String> {
    private boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) {
      while (running) {
        if (SOURCE_ITERATOR.hasNext()) {
          ctx.collect(SOURCE_ITERATOR.next());
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
