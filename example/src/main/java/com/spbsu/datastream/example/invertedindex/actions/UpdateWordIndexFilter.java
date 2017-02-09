package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.models.WordContainer;
import com.spbsu.datastream.example.invertedindex.models.WordIndex;
import com.spbsu.datastream.example.invertedindex.models.WordOutput;
import com.spbsu.datastream.example.invertedindex.models.WordPagePosition;
import com.spbsu.datastream.example.invertedindex.utils.PagePositionLong;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

import java.util.ArrayList;
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
    final List<WordContainer> output = new ArrayList<>();
    output.add(new WordOutput(wordPagePosition.word(), wordPagePosition.positions(), WordOutput.ActionType.ADD));
    output.add(new WordIndex(wordPagePosition.word(), wordPagePosition.positions()));
    return output.stream();
  }

  private Stream<WordContainer> createOutputStream(WordIndex wordIndex, WordPagePosition wordPagePosition) {
    final int pageId = PagePositionLong.pageId(wordPagePosition.positions().get(0));
    final TLongList removePagePositionList = new TLongArrayList();
    final long initPagePosition = PagePositionLong.createPagePosition(pageId, 0, 0);
    final int initSearchIndex = -wordIndex.positions().binarySearch(initPagePosition) - 1;
    int finalIndex = initSearchIndex;
    for (int i = initSearchIndex; i < wordIndex.positions().size(); i++) {
      final long pagePosition = wordIndex.positions().get(i);
      if (PagePositionLong.pageId(pagePosition) == pageId) {
        final int position = PagePositionLong.position(pagePosition);
        final long initVersionPagePosition = PagePositionLong.createPagePosition(pageId, position, 0);
        final int searchResultIndex = -wordPagePosition.positions().binarySearch(initVersionPagePosition) - 1;
        if (searchResultIndex >= 0 && searchResultIndex < wordPagePosition.positions().size()) {
          final long nearestPagePosition = wordPagePosition.positions().get(searchResultIndex);
          if (PagePositionLong.position(nearestPagePosition) != position) {
            removePagePositionList.add(pagePosition);
          }
        } else {
          removePagePositionList.add(pagePosition);
        }

      } else {
        break;
      }
      finalIndex++;
    }

    final TLongList addPagePositionList = new TLongArrayList();
    wordPagePosition.positions().forEach(pagePosition -> {
      final int position = PagePositionLong.position(pagePosition);
      final long initVersionPagePosition = PagePositionLong.createPagePosition(pageId, position, 0);
      final int searchResultIndex = -wordIndex.positions().binarySearch(initVersionPagePosition) - 1;
      if (searchResultIndex >= 0 && searchResultIndex < wordIndex.positions().size()) {
        final long nearestPagePosition = wordIndex.positions().get(searchResultIndex);
        if (PagePositionLong.position(nearestPagePosition) != position) {
          addPagePositionList.add(pagePosition);
        }
      } else {
        addPagePositionList.add(pagePosition);
      }
      return true;
    });

    final TLongList positions = new TLongArrayList();
    positions.addAll(wordIndex.positions());
    final List<WordContainer> output = new ArrayList<>();
    if (!removePagePositionList.isEmpty()) {
      positions.remove(initSearchIndex, finalIndex - initSearchIndex);
      positions.addAll(wordPagePosition.positions());
      positions.sort();
      output.add(new WordOutput(wordIndex.word(), removePagePositionList, WordOutput.ActionType.REMOVE));
    }
    if (!addPagePositionList.isEmpty()) {
      if (removePagePositionList.isEmpty()) {
        if (addPagePositionList.get(0) < positions.get(positions.size() - 1)) {
          positions.addAll(addPagePositionList);
          positions.sort();
        } else {
          positions.addAll(addPagePositionList);
        }
      }
      output.add(new WordOutput(wordIndex.word(), addPagePositionList, WordOutput.ActionType.ADD));
    }
    output.add(new WordIndex(wordIndex.word(), positions));
    return output.stream();
  }
}
