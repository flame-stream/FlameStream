package com.spbsu.flamestream.example.inverted_index.model;

import java.util.Arrays;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexAdd implements WordContainer {
  private final String word;
  private final long[] positions;

  public WordIndexAdd(String word, long[] positions) {
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
    return "ADD " + word + " : " + Arrays.toString(positions);
  }
}
