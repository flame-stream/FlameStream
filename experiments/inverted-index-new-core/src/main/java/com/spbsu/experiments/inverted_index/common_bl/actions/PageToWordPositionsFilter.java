package com.spbsu.experiments.inverted_index.common_bl.actions;

import com.spbsu.commons.text.lexical.WordsTokenizer;
import com.spbsu.experiments.inverted_index.common_bl.models.WikiPage;
import com.spbsu.experiments.inverted_index.common_bl.models.WordContainer;
import com.spbsu.experiments.inverted_index.common_bl.models.WordIndex;
import com.spbsu.experiments.inverted_index.common_bl.models.WordPagePosition;
import com.spbsu.experiments.inverted_index.common_bl.utils.PagePositionLong;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class PageToWordPositionsFilter implements Function<WordContainer, Stream<WordContainer>> {

  @Override
  public Stream<WordContainer> apply(WordContainer container) {
    if (container instanceof WikiPage) {
      final WikiPage wikiPage = (WikiPage) container;
      final WordsTokenizer tokenizer = new WordsTokenizer(wikiPage.titleAndText());
      final Map<String, TLongList> wordPositions = new HashMap<>();
      final List<WordContainer> wordPagePositions = new ArrayList<>();
      int position = 0;
      while (tokenizer.hasNext()) {
        final String word = ((String) tokenizer.next()).toLowerCase();
        final long pagePosition = PagePositionLong.createPagePosition(wikiPage.id(), position, wikiPage.version());
        if (!wordPositions.containsKey(word)) {
          final TLongList positions = new TLongArrayList();
          positions.add(pagePosition);
          wordPositions.put(word, positions);
        } else {
          wordPositions.get(word).add(pagePosition);
        }
        position++;
      }
      wordPositions.forEach((word, list) -> wordPagePositions.add(new WordPagePosition(word, list.toArray())));
      return wordPagePositions.stream();
    } else if (container instanceof WordIndex) {
      return Stream.of(container);
    }
    return Stream.empty();
  }
}