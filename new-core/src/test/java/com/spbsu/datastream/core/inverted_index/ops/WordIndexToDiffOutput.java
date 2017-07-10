package com.spbsu.datastream.core.inverted_index.ops;

import com.spbsu.datastream.core.inverted_index.model.*;
import com.spbsu.datastream.core.inverted_index.utils.IndexLongUtil;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexToDiffOutput implements Function<List<WordContainer>, Stream<WordContainer>> {
  @Override
  public Stream<WordContainer> apply(List<WordContainer> wordContainers) {
    final WordContainer first = wordContainers.get(0);
    if (wordContainers.size() < 2) {
      final WordPagePositions wordPagePosition = (WordPagePositions) first;
      return createOutputStream(new WordIndex(wordPagePosition.word(), new InvertedIndexState()), wordPagePosition);
    } else {
      final WordContainer second = wordContainers.get(1);
      if (first instanceof WordIndex && second instanceof WordPagePositions) {
        return createOutputStream((WordIndex) first, (WordPagePositions) second);
      } else {
        return null;
      }
    }
  }

  private Stream<WordContainer> createOutputStream(WordIndex wordIndex, WordPagePositions wordPagePosition) {
    WordIndexRemove wordRemoveOutput = null;
    final long prevValue = wordIndex.state().updateOrInsert(wordPagePosition.positions());
    if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
      wordRemoveOutput = new WordIndexRemove(wordIndex.word(), IndexLongUtil.setRange(prevValue, 0), IndexLongUtil.range(prevValue));
    }

    final WordIndex newWordIndex = new WordIndex(wordIndex.word(), wordIndex.state());
    final WordIndexAdd wordAddOutput = new WordIndexAdd(wordIndex.word(), wordPagePosition.positions());
    return Stream.of(newWordIndex, wordRemoveOutput, wordAddOutput).filter(Objects::nonNull);
  }
}
