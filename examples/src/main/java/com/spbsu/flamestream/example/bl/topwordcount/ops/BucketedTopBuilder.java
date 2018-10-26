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

public class BucketedTopBuilder extends GroupedReducerBuilder<Object, WordsTop> {
  private final Integer limit;
  private final Integer buckets;

  public BucketedTopBuilder(Integer limit, Integer buckets) {
    super(Object.class, WordsTop.class);
    this.limit = limit;
    this.buckets = buckets;
  }

  protected int groupingHash(Object input) {
    if (input instanceof WordCounter) {
      return ((WordCounter) input).word().hashCode();
    } else {
      return ((WordsTop) input).wordCounters().entrySet().stream().findFirst().get().getKey().hashCode();
    }
  }

  @Override
  protected HashFunction groupingHashFunction() {
    return HashFunction.bucketedHash(super.groupingHashFunction(), buckets);
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
