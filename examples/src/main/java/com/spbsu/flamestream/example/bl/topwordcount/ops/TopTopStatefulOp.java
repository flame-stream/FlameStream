package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class TopTopStatefulOp implements StatefulOp<WordsTop, WordsTop, WordsTop> {
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

  @Override
  public WordsTop aggregate(WordsTop in, @Nullable WordsTop state) {
    if (state == null)
      return in;
    return new WordsTop(Stream.of(in, state).flatMap(wordsTop -> wordsTop.wordCounters().entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max))
            .entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(limit)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, Math::max)));
  }
}
