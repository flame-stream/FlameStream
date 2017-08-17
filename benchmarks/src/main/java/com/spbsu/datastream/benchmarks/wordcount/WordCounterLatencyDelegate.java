package com.spbsu.datastream.benchmarks.wordcount;

import com.spbsu.datastream.benchmarks.LatencyMeasurerDelegate;
import com.spbsu.datastream.core.wordcount.WordCounter;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.*;

public final class WordCounterLatencyDelegate implements LatencyMeasurerDelegate<WordCounter> {
  private final TObjectIntMap<String> expected = new TObjectIntHashMap<>();
  private final TObjectIntMap<String> received = new TObjectIntHashMap<>();
  private final Set<String> inProcess = new HashSet<>();

  @Override
  public void onStart(WordCounter key) {
    expected.adjustOrPutValue(key.word(), 1, 1);
  }

  @Override
  public void onProcess(WordCounter key) {
    final String word = key.word();
    System.out.println("Measuring at " + word + ':' + expected.get(word));
    inProcess.add(word);
  }

  @Override
  public void onFinish(WordCounter key, long latency) {
    final String word = key.word();
    if (key.count() > received.get(word))
      received.put(word, key.count());
    if (inProcess.contains(word) && expected.get(word) == received.get(word)) {
      System.out.println("Measured at " + word + ':' + key.count() + ' ' + latency + " nanoseconds");
      inProcess.remove(word);
    }
  }

  @Override
  public void onStopMeasure() {
    if (!received.equals(expected)) {
      throw new IllegalArgumentException("Everything sucks");
    }
  }
}
