package com.spbsu.flamestream.example.inverted_index.ops;

import com.spbsu.flamestream.example.inverted_index.model.WordBase;
import com.spbsu.flamestream.example.inverted_index.model.WordIndex;

import java.util.function.Predicate;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexFilter implements Predicate<WordBase> {
  @Override
  public boolean test(WordBase wordBase) {
    return !(wordBase instanceof WordIndex);
  }
}
