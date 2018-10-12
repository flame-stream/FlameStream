package com.spbsu.flamestream.example.bl.topwordcount.model;

import java.util.Map;

public class WordsTop {
  private final Map<String, Integer> wordCounters;

  public WordsTop(Map<String, Integer> wordCounters) {
    this.wordCounters = wordCounters;
  }

  public Map<String, Integer> wordCounters() {
    return wordCounters;
  }

  @Override
  public String toString() { return wordCounters.toString(); }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WordsTop that = (WordsTop) o;
    return wordCounters.equals(that.wordCounters());
  }
}
