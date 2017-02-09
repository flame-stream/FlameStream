package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.commons.text.lexical.WordsTokenizer;
import com.spbsu.datastream.example.invertedindex.models.WikiPage;
import com.spbsu.datastream.example.invertedindex.models.WordContainer;
import com.spbsu.datastream.example.invertedindex.models.WordIndex;
import com.spbsu.datastream.example.invertedindex.models.WordPagePosition;
import com.spbsu.datastream.example.invertedindex.utils.PagePositionLong;
import com.spbsu.datastream.example.invertedindex.utils.PageVersions;
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
      final int pageVersion = PageVersions.updateVersion(wikiPage.id());
      final WordsTokenizer tokenizer = new WordsTokenizer(wikiPage.titleAndText());
      final Map<String, TLongList> wordPositions = new HashMap<>();
      final List<WordContainer> wordPagePositions = new ArrayList<>();
      int position = 0;
      while (tokenizer.hasNext()) {
        final String word = ((String) tokenizer.next()).toLowerCase();
        final long pagePosition = PagePositionLong.createPagePosition(wikiPage.id(), position, pageVersion);
        if (!wordPositions.containsKey(word)) {
          TLongList positions = new TLongArrayList();
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
      final List<WordContainer> wrapper = new ArrayList<>();
      wrapper.add(container);
      return wrapper.stream();
    }
    return null;
  }
}