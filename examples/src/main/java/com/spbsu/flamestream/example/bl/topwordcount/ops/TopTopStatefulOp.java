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

  public Class<WordsTop> inputClass() {
    return WordsTop.class;
  }

  public Class<WordsTop> outputClass() {
    return WordsTop.class;
  }

  public int groupingHash(WordsTop input) {
    return 0;
  }

  @Override
  public HashFunction groupingHashFunction(HashFunction hashFunction) {
    return HashFunction.constantHash(0);
  }

  @Override
  @SuppressWarnings("Convert2Lambda")
  public Equalz groupingEqualz(Equalz equalz) {
    return new Equalz() {
      @Override
      public boolean test(DataItem o1, DataItem o2) {
        return true;
      }
    };
  }

  public boolean groupingEquals(WordsTop left, WordsTop right) {
    return true;
  }

  public WordsTop output(WordsTop input) {
    return input;
  }

  public WordsTop reduce(WordsTop left, WordsTop right) {
    return new WordsTop(Stream.of(left, right).flatMap(wordsTop -> wordsTop.wordCounters().entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max))
            .entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(limit)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max)));
  }
}
