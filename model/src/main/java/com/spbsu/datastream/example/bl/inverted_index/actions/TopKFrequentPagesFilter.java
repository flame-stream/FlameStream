package com.spbsu.datastream.example.bl.inverted_index.actions;

import com.spbsu.datastream.core.Filter;
import com.spbsu.datastream.example.bl.inverted_index.WordIndex;

/**
 * Author: Artem
 * Date: 23.01.2017
 */
public class TopKFrequentPagesFilter implements Filter<WordIndex, WordIndex> {

  private final int k;

  public TopKFrequentPagesFilter(int k) {
    this.k = k;
  }

  @Override
  public boolean processOutputByElement() {
    return false;
  }

  @Override
  public WordIndex apply(WordIndex wordIndex) {
    if (wordIndex.pages().size() > k) {
      wordIndex.pages().poll();
    }
    return wordIndex;
  }
}
