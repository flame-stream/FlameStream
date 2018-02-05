package com.spbsu.flamestream.example.bl.index.model;

import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WordPagePositions that = (WordPagePositions) o;
    return Objects.equals(word, that.word) &&
            (IndexItemInLong.pageId(positions[0]) == IndexItemInLong.pageId(that.positions[0]));
  }

  @Override
  public int hashCode() {
    return Objects.hash(word, IndexItemInLong.pageId(positions[0]));
  }
}
