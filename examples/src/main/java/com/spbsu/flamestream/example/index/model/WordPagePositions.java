package com.spbsu.flamestream.example.index.model;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordPagePositions implements WordBase {
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
