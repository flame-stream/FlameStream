package com.spbsu.datastream.benchmarks.bl.wordcount.ops;

import com.spbsu.datastream.benchmarks.bl.wordcount.model.WordContainer;
import com.spbsu.datastream.benchmarks.bl.wordcount.model.WordCounter;
import com.spbsu.datastream.benchmarks.bl.wordcount.model.WordEntry;

import java.util.List;
import java.util.function.Predicate;

/**
 * User: Artem
 * Date: 25.06.2017
 */
public class WordContainerOrderingFilter implements Predicate<List<WordContainer>> {
  @Override
  public boolean test(List<WordContainer> wordContainers) {
    if (wordContainers.size() > 2)
      throw new IllegalStateException("Group size should be <= 2");
    else if (wordContainers.size() == 1 && !(wordContainers.get(0) instanceof WordEntry))
      throw new IllegalStateException("The only element in group should be WordEntry");

    return wordContainers.size() == 1 || (wordContainers.get(0) instanceof WordCounter && wordContainers.get(1) instanceof WordEntry);
  }
}
