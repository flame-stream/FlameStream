package com.spbsu.flamestream.example.bl.index.ops;

import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordIndex;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexFilter implements FlameMap.SerializableFunction<WordBase, Stream<WordBase>> {

  @Override
  public Stream<WordBase> apply(WordBase wordBase) {
    if (!(wordBase instanceof WordIndex)) {
      return Stream.of(wordBase);
    } else {
      return Stream.empty();
    }
  }
}
