package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.example.bl.topwordcount.model.WordContainer;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordEntry;

public class CounterBuilder extends GroupedReducerBuilder<WordContainer, WordCounter> {
  public CounterBuilder() {
    super(WordContainer.class, WordCounter.class);
  }

  protected int groupingHash(WordContainer input) {
    return input.hashCode();
  }

  protected boolean groupingEquals(WordContainer left, WordContainer right) {
    return left.word().equals(right.word());
  }

  protected WordCounter output(WordContainer input) {
    if (input instanceof WordEntry) {
      return new WordCounter(input.word(), 1);
    } else {
      return (WordCounter) input;
    }
  }

  protected WordCounter reduce(WordCounter left, WordCounter right) {
    return new WordCounter(left.word(), left.count() + right.count());
  }
}
