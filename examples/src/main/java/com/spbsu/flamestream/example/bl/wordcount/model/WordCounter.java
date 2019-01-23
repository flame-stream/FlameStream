package com.spbsu.flamestream.example.bl.wordcount.model;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordDocEntry;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class WordCounter implements WordContainer {
  private final WordEntry wordEntry;
  private final int count;

  public WordCounter(WordEntry wordEntry, int count) {
    this.wordEntry = wordEntry;
    this.count = count;
  }

  @Override
  public String word() {
    return wordEntry.word();
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
    return count == that.count && wordEntry.word().equals(that.wordEntry.word());
  }

  @Override
  public int hashCode() {
    return wordEntry.word().hashCode();
  }

  @Override
  public String toString() {
    return String.format("%s: %d", wordEntry, count);
  }
}
