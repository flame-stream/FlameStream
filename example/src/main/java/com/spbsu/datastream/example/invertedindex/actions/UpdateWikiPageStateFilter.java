package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.commons.text.lexical.WordsTokenizer;
import com.spbsu.datastream.example.invertedindex.*;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class UpdateWikiPageStateFilter implements Function<WikiPageContainer[], Stream<WikiPageContainer>> {

  @Override
  public Stream<WikiPageContainer> apply(WikiPageContainer[] containers) {
    final WikiPageContainer first = containers[0];
    if (containers.length < 2) {
      final WikiPage wikiPage = (WikiPage) first;
      final List<WikiPageContainer> output = new ArrayList<>();
      final TIntObjectMap<TIntSet> positionMap = createPositionMap(wikiPage, output);
      final WikiPageState state = new WikiPageState(wikiPage, positionMap);
      output.add(state);
      return output.stream();
    } else {
      final WikiPageContainer second = containers[1];
      if (first instanceof WikiPageState && second instanceof WikiPage) {
        final WikiPageState wikiPageState = (WikiPageState) first;
        final TIntObjectMap<TIntSet> positionMap = wikiPageState.positionMap();
        final WikiPage wikiPage = (WikiPage) second;
        final TIntObjectMap<TIntSet> updatedPositionMap = createPositionMap(wikiPage);

        final List<WikiPageContainer> output = createOutputFromPositionMaps(wikiPage, positionMap, updatedPositionMap);
        final WikiPageState newState = new WikiPageState(wikiPage, updatedPositionMap);
        output.add(newState);
        return output.stream();
      } else {
        return null;
      }
    }
  }

  private TIntObjectMap<TIntSet> createPositionMap(WikiPage wikiPage) {
    return createPositionMap(wikiPage, null);
  }

  private TIntObjectMap<TIntSet> createPositionMap(WikiPage wikiPage, List<WikiPageContainer> addWordsOutput) {
    final TIntObjectMap<TIntSet> positionMap = new TIntObjectHashMap<>();
    final WordsTokenizer tokenizer = new WordsTokenizer(wikiPage.textAndTitle());
    int position = 0;
    while (tokenizer.hasNext()) {
      final String word = ((String) tokenizer.next()).toLowerCase();
      final int wordId = WordDictionary.getWordId(word);
      if (positionMap.containsKey(wordId)) {
        positionMap.get(wordId).add(position);
      } else {
        final TIntSet set = new TIntHashSet();
        set.add(position);
        positionMap.put(wordId, set);
      }
      if (addWordsOutput != null) {
        addWordsOutput.add(new WordOutput(word, wikiPage.pageId(), position, WordOutput.ActionType.ADD));
      }
      position++;
    }
    return positionMap;
  }

  private List<WikiPageContainer> createOutputFromPositionMaps(WikiPage wikiPage, TIntObjectMap<TIntSet> initMap, TIntObjectMap<TIntSet> updatedMap) {
    final List<WikiPageContainer> output = new ArrayList<>();
    updatedMap.forEachEntry((key, set) -> {
      if (initMap.get(key) == null || initMap.get(key).isEmpty()) {
        set.forEach(pos -> {
          final String word = WordDictionary.getWordById(key);
          output.add(new WordOutput(word, wikiPage.pageId(), pos, WordOutput.ActionType.ADD));
          return true;
        });
      } else {
        set.forEach(pos -> {
          if (!initMap.get(key).contains(pos)) {
            final String word = WordDictionary.getWordById(key);
            output.add(new WordOutput(word, wikiPage.pageId(), pos, WordOutput.ActionType.ADD));
          } else {
            initMap.get(key).remove(pos);
          }
          return true;
        });
      }
      return true;
    });
    initMap.forEachEntry((key, set) -> {
      set.forEach(pos -> {
        final String word = WordDictionary.getWordById(key);
        output.add(new WordOutput(word, wikiPage.pageId(), pos, WordOutput.ActionType.REMOVE));
        return true;
      });
      return true;
    });
    return output;
  }
}
