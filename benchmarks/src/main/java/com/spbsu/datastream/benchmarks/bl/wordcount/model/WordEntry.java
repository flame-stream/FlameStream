package com.spbsu.datastream.benchmarks.bl.wordcount.model;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class WordEntry implements WordContainer {
  private final String word;

  public WordEntry(String word) {
    this.word = word;
  }

  @Override
  public String word() {
    return word;
  }

  @Override
  public String toString() {
    return word;
  }
}
