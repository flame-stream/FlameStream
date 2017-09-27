package com.spbsu.flamestream.example.inverted_index.ops;

import com.spbsu.flamestream.example.inverted_index.model.WordContainer;
import com.spbsu.flamestream.example.inverted_index.model.WordIndex;
import com.spbsu.flamestream.example.inverted_index.model.WordPagePositions;

import java.util.List;
import java.util.function.Predicate;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WrongOrderingFilter implements Predicate<List<WordContainer>> {

  @Override
  public boolean test(List<WordContainer> wordContainers) {
    if (wordContainers.size() > 2)
      throw new IllegalStateException("Group size should be <= 2");
    else if (wordContainers.size() == 1 && !(wordContainers.get(0) instanceof WordPagePositions))
      throw new IllegalStateException("The only element in group should be WordPagePosition");

    return wordContainers.size() == 1 || (wordContainers.get(0) instanceof WordIndex && wordContainers.get(1) instanceof WordPagePositions);
  }
}
