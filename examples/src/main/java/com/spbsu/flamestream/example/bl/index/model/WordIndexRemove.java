package com.spbsu.flamestream.example.bl.index.model;

import java.util.Objects;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexRemove implements WordBase {
  private final String word;
  private final long start;
  private final int range;

  public WordIndexRemove(String word,
                         long start,
                         int range) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WordIndexRemove that = (WordIndexRemove) o;
    return start == that.start &&
            range == that.range &&
            Objects.equals(word, that.word);
  }

  @Override
  public int hashCode() {
    return word.hashCode();
  }
}
