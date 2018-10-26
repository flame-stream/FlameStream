package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class TopTopBuilder extends GroupedReducerBuilder<WordsTop, WordsTop> {
  private final Integer limit;

  public TopTopBuilder(Integer limit) {
    super(WordsTop.class, WordsTop.class);
    this.limit = limit;
  }

  protected int groupingHash(WordsTop input) {
    return 0;
  }

  @Override
  protected HashFunction groupingHashFunction() {
    return HashFunction.constantHash(0);
  }

  @Override
  @SuppressWarnings("Convert2Lambda")
  protected Equalz groupingEqualz() {
    return new Equalz() {
      @Override
      public boolean test(DataItem o1, DataItem o2) {
        return true;
      }
    };
  }

  protected boolean groupingEquals(WordsTop left, WordsTop right) {
    return true;
  }

  protected WordsTop output(WordsTop input) {
    return input;
  }

  protected WordsTop reduce(WordsTop left, WordsTop right) {
    return new WordsTop(Stream.of(left, right).flatMap(wordsTop -> wordsTop.wordCounters().entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max))
            .entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(limit)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max)));
  }
}
