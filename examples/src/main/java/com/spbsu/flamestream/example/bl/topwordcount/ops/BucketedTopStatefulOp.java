package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class BucketedTopStatefulOp implements StatefulOp<WordCounter, WordsTop, WordsTop> {
  private final int limit;

  public BucketedTopStatefulOp(int limit) {
    this.limit = limit;
  }

  public Class<WordCounter> inputClass() {
    return WordCounter.class;
  }

  public Class<WordsTop> outputClass() {
    return WordsTop.class;
  }

  @Override
  public WordsTop aggregate(WordCounter wordCounter, @Nullable WordsTop state) {
    final HashMap<String, Integer> wordCounters = new HashMap<>();
    wordCounters.put(wordCounter.word(), wordCounter.count());
    final WordsTop singleWordTop = new WordsTop(wordCounters);
    if (state == null)
      return singleWordTop;
    return new WordsTop(Stream.of(singleWordTop, state)
            .flatMap(wordsTop -> wordsTop.wordCounters().entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max))
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .limit(limit)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max)));
  }
}
