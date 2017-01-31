package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.commons.text.lexical.WordsTokenizer;
import com.spbsu.datastream.example.invertedindex.WordContainer;
import com.spbsu.datastream.example.invertedindex.WordIndex;
import com.spbsu.datastream.example.invertedindex.WordPage;
import com.spbsu.datastream.example.invertedindex.wiki.WikiPage;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class PageToWordsFilter implements Function<WordContainer, Stream<WordContainer>> {

  @Override
  public Stream<WordContainer> apply(WordContainer container) {
    if (container instanceof WikiPage) {
      final WikiPage wikiPage = (WikiPage) container;
      final TObjectIntHashMap<String> wordCount = new TObjectIntHashMap<>();
      int totalWordCount = 0;

      final WordsTokenizer titleTokenizer = new WordsTokenizer(wikiPage.title());
      while (titleTokenizer.hasNext()) {
        String word = ((String) titleTokenizer.next()).toLowerCase();
        wordCount.adjustOrPutValue(word, 1, 1);
        totalWordCount++;
      }
      final WordsTokenizer textTokenizer = new WordsTokenizer(wikiPage.text());
      while (textTokenizer.hasNext()) {
        String word = ((String) textTokenizer.next()).toLowerCase();
        wordCount.adjustOrPutValue(word, 1, 1);
        totalWordCount++;
      }

      final List<WordContainer> words = new ArrayList<>();
      final TObjectIntIterator<String> iterator = wordCount.iterator();
      while (iterator.hasNext()) {
        iterator.advance();
        words.add(new WordPage(wikiPage.id(), iterator.key(), (double) iterator.value() / (double) totalWordCount));
      }
      return words.stream();
    } else if (container instanceof WordIndex) {
      final List<WordContainer> wrapper = new ArrayList<>();
      wrapper.add(container);
      return wrapper.stream();
    }
    return null;
  }
}