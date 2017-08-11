package com.spbsu.datastream.core.wordcount;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class WordCounter implements WordContainer {
  private final String word;
  private final int count;

  public WordCounter(String word, int count) {
    this.word = word;
    this.count = count;
  }

  @Override
  public String word() {
    return word;
  }

  public int count() {
    return count;
  }
}
