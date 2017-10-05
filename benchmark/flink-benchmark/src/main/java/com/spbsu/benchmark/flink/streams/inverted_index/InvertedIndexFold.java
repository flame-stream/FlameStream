package com.spbsu.benchmark.flink.streams.inverted_index;

import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexRemove;
import com.spbsu.flamestream.example.inverted_index.ops.InvertedIndexState;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class InvertedIndexFold {
  private final InvertedIndexState state;
  private final WordIndexAdd wordIndexAdd;
  private final WordIndexRemove wordIndexRemove;

  public InvertedIndexFold(InvertedIndexState state, WordIndexAdd wordIndexAdd, WordIndexRemove wordIndexRemove) {
    this.state = state;
    this.wordIndexAdd = wordIndexAdd;
    this.wordIndexRemove = wordIndexRemove;
  }

  public InvertedIndexState state() {
    return state;
  }

  public WordIndexAdd wordIndexAdd() {
    return wordIndexAdd;
  }

  public WordIndexRemove wordIndexRemove() {
    return wordIndexRemove;
  }
}
