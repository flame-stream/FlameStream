package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.models.*;
import com.spbsu.datastream.example.invertedindex.models.long_containers.IndexLongContainer;
import com.spbsu.datastream.example.invertedindex.models.long_containers.PageLongContainer;

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
    final IndexLongContainer[] indexPositions = new IndexLongContainer[1];
    indexPositions[0] = new IndexLongContainer(wordPagePosition.positions());

    final List<WordContainer> output = new ArrayList<>();
    output.add(new WordIndex(wordPagePosition.word(), indexPositions));
    output.add(new WordAddOutput(wordPagePosition.word(), wordPagePosition.positions()));
    return output.stream();
  }

  private Stream<WordContainer> createOutputStream(WordIndex wordIndex, WordPagePosition wordPagePosition) {
    final IndexLongContainer newValue = new IndexLongContainer(wordPagePosition.positions());
    final IndexLongContainer[] indexPositions = wordIndex.positions();
    final IndexLongContainer positionForSearch = new IndexLongContainer(newValue.pageId(), 0, 0);
    final int nearestPositionIndex = -Arrays.binarySearch(indexPositions, positionForSearch) - 1;
    final boolean updateNotInsert = nearestPositionIndex < indexPositions.length && indexPositions[nearestPositionIndex].pageId() == newValue.pageId();
    final List<WordContainer> output = new ArrayList<>();
    if (updateNotInsert) {
      final IndexLongContainer[] updatedPositions = new IndexLongContainer[indexPositions.length];
      System.arraycopy(indexPositions, 0, updatedPositions, 0, indexPositions.length);
      updatedPositions[nearestPositionIndex] = newValue;
      output.add(new WordIndex(wordIndex.word(), updatedPositions));

      final IndexLongContainer nearestPosition = indexPositions[nearestPositionIndex];
      final int range = nearestPosition.range();
      final PageLongContainer start = new PageLongContainer(nearestPosition.pageId(), nearestPosition.version(), nearestPosition.position());
      output.add(new WordRemoveOutput(wordIndex.word(), start, range));
    } else {
      final IndexLongContainer[] updatedPositions = new IndexLongContainer[indexPositions.length + 1];
      System.arraycopy(indexPositions, 0, updatedPositions, 0, nearestPositionIndex);
      System.arraycopy(indexPositions, nearestPositionIndex, updatedPositions, nearestPositionIndex + 1, indexPositions.length - nearestPositionIndex);
      updatedPositions[nearestPositionIndex] = newValue;
      output.add(new WordIndex(wordIndex.word(), updatedPositions));
    }
    output.add(new WordAddOutput(wordIndex.word(), wordPagePosition.positions()));
    return output.stream();
  }
}