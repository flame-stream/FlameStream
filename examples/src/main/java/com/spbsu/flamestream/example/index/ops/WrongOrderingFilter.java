package com.spbsu.flamestream.example.index.ops;

import com.spbsu.flamestream.example.index.model.WordBase;
import com.spbsu.flamestream.example.index.model.WordIndex;
import com.spbsu.flamestream.example.index.model.WordPagePositions;

import java.util.List;
import java.util.function.Predicate;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WrongOrderingFilter implements Predicate<List<WordBase>> {

  @Override
  public boolean test(List<WordBase> wordBases) {
    if (wordBases.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    } else if (
      wordBases.size() == 1 && !(wordBases.get(0) instanceof WordPagePositions)) {
      throw new IllegalStateException("The only element in group should be WordPagePosition");
    }

    return wordBases.size() == 1 || (wordBases.get(0) instanceof WordIndex
      && wordBases.get(1) instanceof WordPagePositions);
  }
}
