package com.spbsu.flamestream.example.bl.index.model;

import com.spbsu.flamestream.example.bl.index.ops.InvertedIndexState;

import java.util.Objects;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndex implements WordBase {
  private final String word;
  private final InvertedIndexState state;

  public WordIndex(String word, InvertedIndexState state) {
    this.word = word;
    this.state = state;
  }

  @Override
  public String word() {
    return word;
  }

  public InvertedIndexState state() {
    return state;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WordIndex wordIndex = (WordIndex) o;
    return Objects.equals(word, wordIndex.word) &&
            Objects.equals(state, wordIndex.state);
  }

  @Override
  public int hashCode() {
    return word.hashCode();
  }
}
