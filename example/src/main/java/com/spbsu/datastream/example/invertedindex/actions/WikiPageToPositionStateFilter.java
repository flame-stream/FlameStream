package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.commons.text.lexical.WordsTokenizer;
import com.spbsu.datastream.example.invertedindex.WikiPage;
import com.spbsu.datastream.example.invertedindex.WikiPagePositionState;
import com.spbsu.datastream.example.invertedindex.WordDictionary;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.function.Function;

/**
 * Author: Artem
 * Date: 02.02.2017
 */
public class WikiPageToPositionStateFilter implements Function<WikiPage, WikiPagePositionState> {
  @Override
  public WikiPagePositionState apply(WikiPage wikiPage) {
    TIntObjectMap<TIntSet> positionState = createPositionMap(wikiPage);
    return new WikiPagePositionState(wikiPage, positionState);
  }

  private TIntObjectMap<TIntSet> createPositionMap(WikiPage wikiPage) {
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
      position++;
    }
    return positionMap;
  }
}
