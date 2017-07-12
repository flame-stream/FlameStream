package com.spbsu.datastream.core.inverted_index.datastreams.ops;

import com.spbsu.datastream.core.inverted_index.datastreams.model.WordContainer;
import com.spbsu.datastream.core.inverted_index.datastreams.model.WordIndex;

import java.util.function.Predicate;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexFilter implements Predicate<WordContainer> {
  @Override
  public boolean test(WordContainer wordContainer) {
    return !(wordContainer instanceof WordIndex);
  }
}
