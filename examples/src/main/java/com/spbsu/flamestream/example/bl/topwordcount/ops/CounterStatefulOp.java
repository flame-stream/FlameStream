package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.example.bl.topwordcount.model.WordContainer;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordEntry;
import org.jetbrains.annotations.Nullable;

public class CounterStatefulOp implements StatefulOp<WordContainer, WordCounter, WordCounter> {
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

  public WordCounter aggregate(WordContainer wordContainer, @Nullable WordCounter wordCounter) {
    if (wordCounter == null)
      return new WordCounter(wordContainer.word(), 1);
    else
      return new WordCounter(wordContainer.word(), wordCounter.count() + 1);
  }
}
