package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.example.bl.topwordcount.model.WordContainer;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordEntry;

public class CounterStatefulOp implements StatefulOp<WordContainer, WordCounter> {
  public Class<WordContainer> inputClass() {
    return WordContainer.class;
  }

  public Class<WordCounter> outputClass() {
    return WordCounter.class;
  }

  public WordCounter output(WordContainer input) {
    if (input instanceof WordEntry) {
      return new WordCounter(input.word(), 1);
    } else {
      return (WordCounter) input;
    }
  }

  public WordCounter reduce(WordCounter left, WordCounter right) {
    return new WordCounter(left.word(), left.count() + right.count());
  }
}
