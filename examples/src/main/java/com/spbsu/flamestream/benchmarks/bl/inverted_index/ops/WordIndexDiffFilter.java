package com.spbsu.flamestream.benchmarks.bl.inverted_index.ops;

import com.spbsu.flamestream.benchmarks.bl.inverted_index.model.WordContainer;
import com.spbsu.flamestream.benchmarks.bl.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.benchmarks.bl.inverted_index.model.WordIndexRemove;

import java.util.function.Predicate;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexDiffFilter implements Predicate<WordContainer> {
  @Override
  public boolean test(WordContainer wordContainer) {
    return !(wordContainer instanceof WordIndexAdd || wordContainer instanceof WordIndexRemove);
  }
}
