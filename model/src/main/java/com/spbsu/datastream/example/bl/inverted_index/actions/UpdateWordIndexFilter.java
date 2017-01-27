package com.spbsu.datastream.example.bl.inverted_index.actions;

import com.spbsu.datastream.example.bl.inverted_index.WordContainer;
import com.spbsu.datastream.example.bl.inverted_index.WordIndex;
import com.spbsu.datastream.example.bl.inverted_index.WordPage;

import java.util.function.Function;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class UpdateWordIndexFilter implements Function<WordContainer[], WordIndex> {

  @Override
  public WordIndex apply(WordContainer[] containers) {
    final WordContainer first = containers[0];
    if (containers.length < 2)
      return new WordIndex((WordPage) first);
    else {
      final WordContainer second = containers[1];
      if (first instanceof WordIndex && second instanceof WordPage)
        return new WordIndex((WordIndex) first, (WordPage) second);
      else
        return null;
    }
  }
}
