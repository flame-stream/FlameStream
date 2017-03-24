package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.models.*;
import com.spbsu.datastream.example.invertedindex.utils.InvertedIndexStorage;
import com.spbsu.datastream.example.invertedindex.utils.PagePositionLong;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class UpdateWordIndexFilter implements Function<WordContainer[], Stream<WordContainer>> {

  @Override
  public Stream<WordContainer> apply(WordContainer[] containers) {
    final WordContainer first = containers[0];
    if (containers.length < 2) {
      final WordPagePosition wordPagePosition = (WordPagePosition) first;
      return createOutputStream(new WordIndex(wordPagePosition.word(), new InvertedIndexStorage()), wordPagePosition);
    } else {
      final WordContainer second = containers[1];
      if (first instanceof WordIndex && second instanceof WordPagePosition) {
        return createOutputStream((WordIndex) first, (WordPagePosition) second);
      } else {
        return null;
      }
    }
  }

  private Stream<WordContainer> createOutputStream(WordIndex wordIndex, WordPagePosition wordPagePosition) {
    WordRemoveOutput wordRemoveOutput = null;
    final long prevValue = wordIndex.storage().updateOrInsert(wordPagePosition.positions());
    if (prevValue != InvertedIndexStorage.PREV_VALUE_NOT_FOUND) {
      wordRemoveOutput = new WordRemoveOutput(wordIndex.word(), PagePositionLong.setRange(prevValue, 0), PagePositionLong.range(prevValue));
    }

    final WordIndex newWordIndex = new WordIndex(wordIndex.word(), wordIndex.storage());
    final WordAddOutput wordAddOutput = new WordAddOutput(wordIndex.word(), wordPagePosition.positions());
    return Stream.of(newWordIndex, wordRemoveOutput, wordAddOutput).filter(Objects::nonNull);
  }
}