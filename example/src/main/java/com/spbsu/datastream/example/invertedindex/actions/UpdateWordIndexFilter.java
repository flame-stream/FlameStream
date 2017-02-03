package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.WordContainer;
import com.spbsu.datastream.example.invertedindex.WordIndex;
import com.spbsu.datastream.example.invertedindex.WordPage;

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
