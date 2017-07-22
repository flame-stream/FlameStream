package com.spbsu.datastream.core.inverted_index.datastreams.ops;

import com.spbsu.commons.text.lexical.WordsTokenizer;
import com.spbsu.datastream.core.inverted_index.datastreams.model.WikipediaPage;
import com.spbsu.datastream.core.inverted_index.datastreams.model.WordPagePositions;
import com.spbsu.datastream.core.inverted_index.datastreams.utils.IndexLongUtil;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WikipediaPageToWordPositions implements Function<WikipediaPage, Stream<WordPagePositions>> {
  @Override
  public Stream<WordPagePositions> apply(WikipediaPage wikipediaPage) {
    final WordsTokenizer tokenizer = new WordsTokenizer(wikipediaPage.text());
    final Map<String, TLongList> wordPositions = new HashMap<>();
    final List<WordPagePositions> wordPagePositions = new ArrayList<>();
    int position = 0;
    while (tokenizer.hasNext()) {
      final String word = ((String) tokenizer.next()).toLowerCase();
      final long pagePosition = IndexLongUtil.createPagePosition(wikipediaPage.id(), position, wikipediaPage.version());
      if (!wordPositions.containsKey(word)) {
        final TLongList positions = new TLongArrayList();
        positions.add(pagePosition);
        wordPositions.put(word, positions);
      } else {
        wordPositions.get(word).add(pagePosition);
      }
      position++;
    }
    wordPositions.forEach((word, list) -> wordPagePositions.add(new WordPagePositions(word, list.toArray())));
    return wordPagePositions.stream();
  }
}
