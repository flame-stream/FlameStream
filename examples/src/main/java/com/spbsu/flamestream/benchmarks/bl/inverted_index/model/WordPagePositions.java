package com.spbsu.flamestream.benchmarks.bl.inverted_index.model;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordPagePositions implements WordContainer {
  private final String word;
  private final long[] positions;

  public WordPagePositions(String word, long[] positions) {
    this.word = word;
    this.positions = positions;
  }

  @Override
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
