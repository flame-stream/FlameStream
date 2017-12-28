package com.spbsu.flamestream.example.bl.index.ops;

import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordIndex;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.model.WordPagePositions;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexToDiffOutput implements Function<List<WordBase>, Stream<WordBase>> {
  @Override
  public Stream<WordBase> apply(List<WordBase> wordBases) {
    final WordBase first = wordBases.get(0);
    if (wordBases.size() < 2) {
      final WordPagePositions wordPagePosition = (WordPagePositions) first;
      return createOutputStream(new WordIndex(wordPagePosition.word(), new InvertedIndexState()), wordPagePosition);
    } else {
      final WordBase second = wordBases.get(1);
      if (first instanceof WordIndex && second instanceof WordPagePositions) {
        return createOutputStream((WordIndex) first, (WordPagePositions) second);
      } else {
        throw new IllegalStateException("Wrong order of objects");
      }
    }
  }

  private Stream<WordBase> createOutputStream(WordIndex wordIndex, WordPagePositions wordPagePosition) {
    WordIndexRemove wordRemoveOutput = null;
    final InvertedIndexState newState = wordIndex.state().copy();
    final long prevValue = newState.updateOrInsert(wordPagePosition.positions());
    if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
      wordRemoveOutput = new WordIndexRemove(
              wordIndex.word(),
              IndexItemInLong.setRange(prevValue, 0),
              IndexItemInLong.range(prevValue)
      );
    }

    final WordIndex newWordIndex = new WordIndex(wordIndex.word(), newState);
    final WordIndexAdd wordAddOutput = new WordIndexAdd(wordIndex.word(), wordPagePosition.positions());
    return Stream.of(newWordIndex, wordRemoveOutput, wordAddOutput).filter(Objects::nonNull);
  }
}

