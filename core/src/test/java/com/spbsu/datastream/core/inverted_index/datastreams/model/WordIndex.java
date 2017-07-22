package com.spbsu.datastream.core.inverted_index.datastreams.model;

import com.spbsu.datastream.core.inverted_index.datastreams.ops.InvertedIndexState;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndex implements WordContainer {
  private final String word;
  private final InvertedIndexState state;

  public WordIndex(String word, InvertedIndexState state) {
    this.word = word;
    this.state = state;
  }
  @Override
  public String word() {
    return word;
  }

  public InvertedIndexState state() {
    return state;
  }
}
