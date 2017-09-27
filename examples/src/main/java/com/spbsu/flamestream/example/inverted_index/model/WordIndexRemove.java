package com.spbsu.flamestream.example.inverted_index.model;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexRemove implements WordContainer {
  private final String word;
  private final long start;
  private final int range;

  public WordIndexRemove(String word, long start, int range) {
    this.word = word;
    this.start = start;
    this.range = range;
  }

  @Override
  public String word() {
    return word;
  }

  public long start() {
    return start;
  }

  public int range() {
    return range;
  }

  @Override
  public String toString() {
    return "REMOVE " + word + " : " + start + ", " + range;
  }
}
