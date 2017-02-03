package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.example.invertedindex.WordIndex;

import java.util.function.Function;

/**
 * Author: Artem
 * Date: 23.01.2017
 */
public class TopKFrequentPagesFilter implements Function<WordIndex, WordIndex> {

  private final int k;

  public TopKFrequentPagesFilter(int k) {
    this.k = k;
  }

  @Override
  public WordIndex apply(WordIndex wordIndex) {
    if (wordIndex.pages().size() > k) {
      wordIndex.pages().poll();
    }
    return wordIndex;
  }
}
