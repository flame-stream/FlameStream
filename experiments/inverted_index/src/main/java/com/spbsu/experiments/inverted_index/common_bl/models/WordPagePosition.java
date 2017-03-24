package com.spbsu.experiments.inverted_index.common_bl.models;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordPagePosition implements WordContainer {
  private final String word;
  private final long[] positions;

  public WordPagePosition(String word, long[] positions) {
    this.word = word;
    this.positions = positions;
  }

  public String word() {
    return word;
  }

  public long[] positions() {
    return positions;
  }

  @Override
  public String toString() {
    return word;
  }
}