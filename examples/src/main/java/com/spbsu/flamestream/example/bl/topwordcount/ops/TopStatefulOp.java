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

public class TopStatefulOp implements StatefulMapOperation<WordCounter, WordsTop, WordsTop> {
  private final Integer limit;

  public TopStatefulOp(Integer limit) {
    this.limit = limit;
  }

  public int groupingHash(Object input) {
    return 0;
  }

  public boolean groupingEquals(Object left, Object right) {
    return true;
  }

  public WordsTop accept(WordCounter wc) {
    final HashMap<String, Integer> wordCounters = new HashMap<>();
    wordCounters.put(wc.word(), wc.count());
    return new WordsTop(wordCounters);
  }

  public WordsTop reduce(WordsTop left, WordsTop right) {
    return new WordsTop(Stream.of(left, right).flatMap(wordsTop -> wordsTop.wordCounters().entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max))
            .entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(limit)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max)));
  }

  @Override
  public WordsTop aggregate(WordCounter in, @Nullable WordsTop wordsTop) {
    return null;
  }
}
