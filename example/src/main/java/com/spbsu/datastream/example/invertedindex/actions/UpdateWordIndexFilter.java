package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.models.*;
import com.spbsu.datastream.example.invertedindex.utils.InvertedIndexStorage;
import com.spbsu.datastream.example.invertedindex.utils.PagePositionLong;

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
    final long first = wordPagePosition.positions()[0];
    final int pageId = PagePositionLong.pageId(first);

    final long valueForSearch = PagePositionLong.createPagePosition(pageId, 0, 0);
    final int newPosition = PagePositionLong.position(first);
    final int newRange = wordPagePosition.positions().length;

    WordRemoveOutput wordRemoveOutput = null;
    final long prevValue = wordIndex.storage().tryToFindAndUpdate(valueForSearch, newPosition, newRange);
    if (prevValue == InvertedIndexStorage.PREV_VALUE_NOT_FOUND) {
      final long newValue = PagePositionLong.setRange(first, newRange);
      wordIndex.storage().insert(newValue);
    } else {
      wordRemoveOutput = new WordRemoveOutput(wordIndex.word(), PagePositionLong.setRange(prevValue, 0), PagePositionLong.range(prevValue));
    }

    final WordAddOutput wordAddOutput = new WordAddOutput(wordIndex.word(), wordPagePosition.positions());
    final WordIndex newWordIndex = new WordIndex(wordIndex.word(), wordIndex.storage());
    if (wordRemoveOutput == null) {
      return Stream.of(wordIndex, wordAddOutput);
    }
    return Stream.of(wordIndex, wordRemoveOutput, wordAddOutput);
  }
}