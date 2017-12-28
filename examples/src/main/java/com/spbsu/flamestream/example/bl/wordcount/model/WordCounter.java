package com.spbsu.flamestream.example.bl.wordcount.model;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WordCounter that = (WordCounter) o;
    return count == that.count && word.equals(that.word);
  }

  @Override
  public int hashCode() {
    return word.hashCode();
  }

  @Override
  public String toString() {
    return word;
  }
}
