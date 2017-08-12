package com.spbsu.datastream.benchmarks.wordcount;

import com.spbsu.datastream.core.wordcount.WordCounter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static java.util.Collections.*;

public final class LatencyMeasurer implements Consumer<Object> {
  private static final Pattern PATTERN = Pattern.compile("\\s");
  private final long warmUpDelay;
  private final long measurePeriod;

  private final Map<String, Integer> expected = new HashMap<>();
  private final Map<String, Measure> measure = new ConcurrentHashMap<>();
  private final Map<String, Integer> received = new HashMap<>();

  private long delay;

  private final List<Long> latencies = new ArrayList<>();

  LatencyMeasurer(long warmUpDelay, long measurePeriod) {
    this.warmUpDelay = warmUpDelay;
    this.measurePeriod = measurePeriod;

    this.delay = warmUpDelay;
  }

  public void logNewText(String text) {
    Arrays.stream(PATTERN.split(text)).forEach(this::logWord);
  }

  public void logWord(String word) {
    expected.merge(word, 1, Integer::sum);
    delay--;

    if (delay <= 0) {
      if (!measure.containsKey(word)) {
        System.out.println("Measuring at " + word + ':' + expected.get(word));
        measure.put(word, new Measure(expected.get(word), System.nanoTime()));
        delay = measurePeriod;
      }
    }
  }

  @Override
  public void accept(Object o) {
    final WordCounter counter = (WordCounter) o;
    this.received.merge(counter.word(), counter.count(), Integer::max);

    final Measure wordMeasure = measure.get(counter.word());
    if (wordMeasure != null) {
      if (wordMeasure.count == this.received.get(counter.word())) {
        final long latency = System.nanoTime() - wordMeasure.startTs;
        System.out.println("Measured at " + counter.word() + ':' + counter.count() + ' ' + latency + " nanoseconds");
        latencies.add(latency);
        measure.remove(counter.word());
      }
    }
  }

  public void assertEverything() {
    if (!received.equals(expected)) {
      throw new IllegalArgumentException("Everything sucks");
    }
  }

  public List<Long> latencies() {
    return unmodifiableList(latencies);
  }

  private static class Measure {
    public final long count;
    public final long startTs;

    public Measure(long count, long startTs) {
      this.count = count;
      this.startTs = startTs;
    }
  }
}
