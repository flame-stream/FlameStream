package com.spbsu.flamestream.example.bl.tfidf.ops.entries;

import com.spbsu.flamestream.example.bl.tfidf.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.WordEntry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class CountWordEntries implements Function<List<WordContainer>, Stream<WordCounter>> {
  @Override
  public Stream<WordCounter> apply(List<WordContainer> wordContainers) {
    if (wordContainers.size() == 1) {
      final WordEntry wordEntry = (WordEntry) wordContainers.get(0);
      return Stream.of(new WordCounter(wordEntry, 1));
    } else {
      final WordCounter counter = (WordCounter) wordContainers.get(0);
      final WordEntry wordEntry = (WordEntry) wordContainers.get(1);
      return Stream.of(new WordCounter(wordEntry, counter.count() + 1));
    }
  }
}