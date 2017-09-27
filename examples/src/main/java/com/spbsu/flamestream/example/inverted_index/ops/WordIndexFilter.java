package com.spbsu.flamestream.example.inverted_index.ops;

import com.spbsu.flamestream.example.inverted_index.model.WordContainer;
import com.spbsu.flamestream.example.inverted_index.model.WordIndex;

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
