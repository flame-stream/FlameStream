package com.spbsu.flamestream.example.bl.wordcount.ops;

import com.spbsu.flamestream.example.bl.wordcount.model.WordContainer;
import com.spbsu.flamestream.example.bl.wordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.wordcount.model.WordEntry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 25.06.2017
 */
public class WordContainerOrderingFilter implements Function<List<WordContainer>, Stream<List<WordContainer>>> {
  @Override
  public Stream<List<WordContainer>> apply(List<WordContainer> wordContainers) {
    if (wordContainers.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    }

    if (wordContainers.size() == 1 && !(wordContainers.get(0) instanceof WordEntry)) {
      throw new IllegalStateException("The only element in group should be WordEntry");
    }

    if (wordContainers.size() == 1 || (wordContainers.get(0) instanceof WordCounter
            && wordContainers.get(1) instanceof WordEntry)) {
      return Stream.of(wordContainers);
    } else {
      return Stream.empty();
    }
  }
}
