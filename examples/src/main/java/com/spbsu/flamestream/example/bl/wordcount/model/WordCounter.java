package com.spbsu.flamestream.example.bl.wordcount.model;

import com.spbsu.flamestream.example.bl.tfidfsd.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidfsd.model.entries.WordDocEntry;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class WordCounter implements WordContainer {
  private final WordDocEntry wordDocEntry;
  private final int count;

  public WordCounter(WordDocEntry wordDocEntry, int count) {
    this.wordDocEntry = wordDocEntry;
    this.count = count;
  }

  @Override
  public String word() {
    return wordDocEntry.word();
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
    return count == that.count && wordDocEntry.word().equals(that.wordDocEntry.word());
  }

  @Override
  public int hashCode() {
    return wordDocEntry.word().hashCode();
  }

  @Override
  public String toString() {
    return String.format("%s: %d", wordDocEntry, count);
  }
}
