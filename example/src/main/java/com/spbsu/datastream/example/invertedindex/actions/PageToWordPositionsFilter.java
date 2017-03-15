package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.commons.text.lexical.WordsTokenizer;
import com.spbsu.datastream.example.invertedindex.models.*;
import com.spbsu.datastream.example.invertedindex.models.long_containers.PageLongContainer;
import com.spbsu.datastream.example.invertedindex.utils.PageVersions;

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
      final Map<String, List<PageLongContainer>> wordPositions = new HashMap<>();
      final List<WordContainer> wordPagePositions = new ArrayList<>();
      int position = 0;
      while (tokenizer.hasNext()) {
        final String word = ((String) tokenizer.next()).toLowerCase();
        final PageLongContainer pagePosition = new PageLongContainer(wikiPage.id(), pageVersion, position);
        if (!wordPositions.containsKey(word)) {
          List<PageLongContainer> positions = new ArrayList<>();
          positions.add(pagePosition);
          wordPositions.put(word, positions);
        } else {
          wordPositions.get(word).add(pagePosition);
        }
        position++;
      }
      wordPositions.forEach((word, list) -> {
        final PageLongContainer[] containers = new PageLongContainer[list.size()];
        for (int i = 0; i < list.size(); i++) {
          containers[i] = list.get(i);
        }
        wordPagePositions.add(new WordPagePosition(word, containers));
      });
      return wordPagePositions.stream();
    } else if (container instanceof WordIndex) {
      final List<WordContainer> wrapper = new ArrayList<>();
      wrapper.add(container);
      return wrapper.stream();
    }
    return null;
  }
}