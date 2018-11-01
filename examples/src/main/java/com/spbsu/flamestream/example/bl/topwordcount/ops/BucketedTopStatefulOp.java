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

public class BucketedTopStatefulOp implements StatefulOp<Object, WordsTop> {
  private final Integer limit;
  private final Integer buckets;

  public BucketedTopStatefulOp(Integer limit, Integer buckets) {
    this.limit = limit;
    this.buckets = buckets;
  }

  public Class<Object> inputClass() {
    return Object.class;
  }

  public Class<WordsTop> outputClass() {
    return WordsTop.class;
  }

  public int groupingHash(Object input) {
    if (input instanceof WordCounter) {
      return ((WordCounter) input).word().hashCode();
    } else {
      return ((WordsTop) input).wordCounters().entrySet().stream().findFirst().get().getKey().hashCode();
    }
  }

  @Override
  public HashFunction groupingHashFunction(HashFunction hashFunction) {
    return HashFunction.bucketedHash(hashFunction, buckets);
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

  public boolean groupingEquals(Object left, Object right) {
    return true;
  }

  public WordsTop output(Object input) {
    if (input instanceof WordCounter) {
      final WordCounter wordCounter = (WordCounter) input;
      final HashMap<String, Integer> wordCounters = new HashMap<>();
      wordCounters.put(wordCounter.word(), wordCounter.count());
      return new WordsTop(wordCounters);
    } else {
      return (WordsTop) input;
    }
  }

  public WordsTop reduce(WordsTop left, WordsTop right) {
    return new WordsTop(Stream.of(left, right).flatMap(wordsTop -> wordsTop.wordCounters().entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max))
            .entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(limit)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max)));
  }
}
