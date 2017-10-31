package com.spbsu.flamestream.example.index.ops;

import com.expleague.commons.text.lexical.StemsTokenizer;
import com.expleague.commons.text.lexical.Tokenizer;
import com.expleague.commons.text.stem.Stemmer;
import com.spbsu.flamestream.example.index.model.WikipediaPage;
import com.spbsu.flamestream.example.index.model.WordPagePositions;
import com.spbsu.flamestream.example.index.utils.IndexItemInLong;
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
    //noinspection deprecation
    final Tokenizer tokenizer = new StemsTokenizer(Stemmer.getInstance(), wikipediaPage.text());
    final Map<String, TLongList> wordPositions = new HashMap<>();
    int position = 0;
    while (tokenizer.hasNext()) {
      final String word = tokenizer.next().toString().toLowerCase();
      final long pagePosition = IndexItemInLong.createPagePosition(
              wikipediaPage.id(),
              position,
              wikipediaPage.version()
      );
      if (!wordPositions.containsKey(word)) {
        final TLongList positions = new TLongArrayList();
        positions.add(pagePosition);
        wordPositions.put(word, positions);
      } else {
        wordPositions.get(word).add(pagePosition);
      }
      position++;
    }
    final List<WordPagePositions> wordPagePositions = new ArrayList<>();
    wordPositions.forEach((word, list) -> wordPagePositions.add(new WordPagePositions(word, list.toArray())));
    return wordPagePositions.stream();
  }
}
