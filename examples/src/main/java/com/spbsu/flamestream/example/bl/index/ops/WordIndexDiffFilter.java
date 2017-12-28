package com.spbsu.flamestream.example.bl.index.ops;

import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexDiffFilter implements Function<WordBase, Stream<WordBase>> {
  @Override
  public Stream<WordBase> apply(WordBase wordBase) {
    if (!(wordBase instanceof WordIndexAdd || wordBase instanceof WordIndexRemove)) {
      return Stream.of(wordBase);
    } else {
      return Stream.empty();
    }
  }
}
