package com.spbsu.flamestream.example.bl.wordcount.ops;

import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordDocEntry;
import com.spbsu.flamestream.example.bl.wordcount.model.WordContainer;
import com.spbsu.flamestream.example.bl.wordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.wordcount.model.WordEntry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 25.06.2017
 */
public class CountWordEntries implements Function<List<WordContainer>, Stream<WordCounter>> {

  @Override
  public Stream<WordCounter> apply(List<WordContainer> wordContainers) {
    if (wordContainers.size() == 1) {
      final WordDocEntry wordDocEntry = (WordDocEntry) wordContainers.get(0);
      return Stream.of(new WordCounter(wordDocEntry, 1));
    } else {
      final WordCounter counter = (WordCounter) wordContainers.get(0);
      final WordDocEntry wordDocEntry = (WordDocEntry) wordContainers.get(1);
      return Stream.of(new WordCounter(wordDocEntry, counter.count() + 1));
    }
  }
}
