package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class TopTopStatefulOp implements StatefulOp<WordsTop, WordsTop> {
  private final Integer limit;

  public TopTopStatefulOp(Integer limit) {
    this.limit = limit;
  }

  public int groupingHash(WordsTop input) {
    return 0;
  }

  public boolean groupingEquals(WordsTop left, WordsTop right) {
    return true;
  }

  public WordsTop reduce(WordsTop left, WordsTop right) {
    return new WordsTop(Stream.of(left, right).flatMap(wordsTop -> wordsTop.wordCounters().entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max))
            .entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(limit)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max)));
  }
}
