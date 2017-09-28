package com.spbsu.flamestream.example.inverted_index.ops;

import com.spbsu.flamestream.example.inverted_index.model.WordBase;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexRemove;

import java.util.function.Predicate;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexDiffFilter implements Predicate<WordBase> {
  @Override
  public boolean test(WordBase wordBase) {
    return !(wordBase instanceof WordIndexAdd || wordBase instanceof WordIndexRemove);
  }
}
