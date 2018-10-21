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

public class TopBuilder extends GroupedReducerBuilder<Object, WordsTop> {
  private final Integer limit;

  public TopBuilder(Integer limit) {
    super(Object.class, WordsTop.class);
    this.limit = limit;
  }

  protected int groupingHash(Object input) {
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

  protected boolean groupingEquals(Object left, Object right) {
    return true;
  }

  protected WordsTop output(Object input) {
    if (input instanceof WordCounter) {
      final WordCounter wordCounter = (WordCounter) input;
      final HashMap<String, Integer> wordCounters = new HashMap<>();
      wordCounters.put(wordCounter.word(), wordCounter.count());
      return new WordsTop(wordCounters);
    } else {
      return (WordsTop) input;
    }
  }

  protected WordsTop reduce(WordsTop left, WordsTop right) {
    return new WordsTop(Stream.of(left, right).flatMap(wordsTop -> wordsTop.wordCounters().entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max))
            .entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(limit)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max)));
  }
}
