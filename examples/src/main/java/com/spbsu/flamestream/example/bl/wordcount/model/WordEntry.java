package com.spbsu.flamestream.example.bl.wordcount.model;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WordEntry wordEntry = (WordEntry) o;
    return Objects.equals(word, wordEntry.word);
  }

  @Override
  public int hashCode() {
    return word.hashCode();
  }
}
