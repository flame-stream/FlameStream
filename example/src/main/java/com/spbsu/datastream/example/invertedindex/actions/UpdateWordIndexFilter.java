package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.models.*;
import com.spbsu.datastream.example.invertedindex.utils.PagePositionLong;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
      return createOutputStream((WordPagePosition) first);
    } else {
      final WordContainer second = containers[1];
      if (first instanceof WordIndex && second instanceof WordPagePosition) {
        return createOutputStream((WordIndex) first, (WordPagePosition) second);
      } else {
        return null;
      }
    }
  }

  private Stream<WordContainer> createOutputStream(WordPagePosition wordPagePosition) {
    final long first = wordPagePosition.positions()[0];
    final long[] compressedPositions = new long[1];
    compressedPositions[0] = PagePositionLong.setRange(first, wordPagePosition.positions().length);

    final List<WordContainer> output = new ArrayList<>();
    output.add(new WordIndex(wordPagePosition.word(), compressedPositions));
    output.add(new WordAddOutput(wordPagePosition.word(), wordPagePosition.positions()));
    return output.stream();
  }

  private Stream<WordContainer> createOutputStream(WordIndex wordIndex, WordPagePosition wordPagePosition) {
    final long first = wordPagePosition.positions()[0];
    final int pageId = PagePositionLong.pageId(first);
    final long newValue = PagePositionLong.setRange(first, wordPagePosition.positions().length);
    final long[] indexPositions = wordIndex.positions();
    final long positionForSearch = PagePositionLong.createPagePosition(pageId, 0, 0);
    final int nearestPositionIndex = -Arrays.binarySearch(indexPositions, positionForSearch) - 1;
    final boolean updateNotInsert = nearestPositionIndex < indexPositions.length && PagePositionLong.pageId(indexPositions[nearestPositionIndex]) == pageId;
    final List<WordContainer> output = new ArrayList<>();
    if (updateNotInsert) {
      final long[] updatedPositions = new long[indexPositions.length];
      System.arraycopy(indexPositions, 0, updatedPositions, 0, indexPositions.length);
      updatedPositions[nearestPositionIndex] = newValue;
      output.add(new WordIndex(wordIndex.word(), updatedPositions));

      final long nearestPosition = indexPositions[nearestPositionIndex];
      final int range = PagePositionLong.range(nearestPosition);
      final long positionWithoutRange = PagePositionLong.setRange(nearestPosition, 0);
      output.add(new WordRemoveOutput(wordIndex.word(), positionWithoutRange, range));
    } else {
      final long[] updatedPositions = new long[indexPositions.length + 1];
      System.arraycopy(indexPositions, 0, updatedPositions, 0, nearestPositionIndex);
      System.arraycopy(indexPositions, nearestPositionIndex, updatedPositions, nearestPositionIndex + 1, indexPositions.length - nearestPositionIndex);
      updatedPositions[nearestPositionIndex] = newValue;
      output.add(new WordIndex(wordIndex.word(), updatedPositions));
    }
    output.add(new WordAddOutput(wordIndex.word(), wordPagePosition.positions()));
    return output.stream();
  }
}