package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public class CountWordsTop implements Function<List<Object>, Stream<Object>> {
  private final Integer limit;

  public CountWordsTop(Integer limit) {
    this.limit = limit;
  }

  @Override
  public Stream<Object> apply(List<Object> wordContainers) {
    if (wordContainers.size() == 1) {
      final WordCounter wordCounter = (WordCounter) wordContainers.get(0);
      final HashMap<String, Integer> wordCounters = new HashMap<>();
      wordCounters.put(wordCounter.word(), wordCounter.count());
      return Stream.of(new WordsTop(wordCounters));
    } else {
      final WordsTop wordsTop = (WordsTop) wordContainers.get(0);
      final WordCounter counter = (WordCounter) wordContainers.get(1);
      if (wordsTop.wordCounters().containsKey(counter.word()) || wordsTop.wordCounters().size() < limit) {
        final HashMap<String, Integer> newWordCounters = new HashMap<>(wordsTop.wordCounters());
        newWordCounters.put(counter.word(), counter.count());
        return Stream.of(new WordsTop(newWordCounters));
      } else {
        final Optional<Map.Entry<String, Integer>> maybeMinWordCounter = wordsTop.wordCounters()
                .entrySet()
                .stream()
                .min(Map.Entry.comparingByValue());
        if (maybeMinWordCounter.isPresent()) {
          final Map.Entry<String, Integer> minWordCounter = maybeMinWordCounter.get();
          if (minWordCounter.getValue() < counter.count()) {
            final HashMap<String, Integer> newWordCounters = new HashMap<>(wordsTop.wordCounters());
            newWordCounters.remove(minWordCounter.getKey());
            newWordCounters.put(counter.word(), counter.count());
            return Stream.of(new WordsTop(newWordCounters));
          } else {
            return Stream.of(wordsTop);
          }
        } else {
          final HashMap<String, Integer> newWordCounters = new HashMap<>(wordsTop.wordCounters());
          newWordCounters.put(counter.word(), counter.count());
          return Stream.of(new WordsTop(newWordCounters));
        }
      }
    }
  }
}
