package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.WikiPagePositionState;
import com.spbsu.datastream.example.invertedindex.WordDictionary;
import com.spbsu.datastream.example.invertedindex.WordOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class ProcessWordOutputFilter implements Function<WikiPagePositionState[], Stream<WordOutput>> {

  @Override
  public Stream<WordOutput> apply(WikiPagePositionState[] states) {
    final WikiPagePositionState olderState = states[0];
    if (states.length < 2) {
      final List<WordOutput> output = new ArrayList<>();
      olderState.positionMap().forEachEntry((key, value) -> {
        value.forEach(pos -> {
          final String word = WordDictionary.getWordById(key);
          output.add(new WordOutput(word, olderState.pageId(), pos, WordOutput.ActionType.ADD));
          return true;
        });
        return true;
      });
      return output.stream();
    } else {
      final WikiPagePositionState newerState = states[1];
      final List<WordOutput> output = createOutputFromPositionMapsDiff(olderState, newerState);
      return output.stream();
    }
  }

  private List<WordOutput> createOutputFromPositionMapsDiff(WikiPagePositionState olderState, WikiPagePositionState newerState) {
    final List<WordOutput> output = new ArrayList<>();
    newerState.positionMap().forEachEntry((key, set) -> {
      if (olderState.positionMap().get(key) == null || olderState.positionMap().get(key).isEmpty()) {
        set.forEach(pos -> {
          final String word = WordDictionary.getWordById(key);
          output.add(new WordOutput(word, newerState.pageId(), pos, WordOutput.ActionType.ADD));
          return true;
        });
      } else {
        set.forEach(pos -> {
          if (!olderState.positionMap().get(key).contains(pos)) {
            final String word = WordDictionary.getWordById(key);
            output.add(new WordOutput(word, newerState.pageId(), pos, WordOutput.ActionType.ADD));
          } else {
            olderState.positionMap().get(key).remove(pos);
          }
          return true;
        });
      }
      return true;
    });
    olderState.positionMap().forEachEntry((key, set) -> {
      set.forEach(pos -> {
        final String word = WordDictionary.getWordById(key);
        output.add(new WordOutput(word, newerState.pageId(), pos, WordOutput.ActionType.REMOVE));
        return true;
      });
      return true;
    });
    return output;
  }
}
