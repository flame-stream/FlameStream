package com.spbsu.flamestream.example.bl.index.model;

import com.google.common.hash.Hashing;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexAdd implements WordBase {
  private final String word;
  private final long[] positions;

  public WordIndexAdd(String word, long[] positions) {
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
    return "ADD " + word + " : " + Arrays.toString(positions);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WordIndexAdd that = (WordIndexAdd) o;
    return Objects.equals(word, that.word) &&
            (IndexItemInLong.pageId(positions[0]) == IndexItemInLong.pageId(that.positions[0]));
  }

  @Override
  public int hashCode() {
    return Hashing.murmur3_32().hashString(word, Charset.forName("UTF-8")).asInt()
            ^ IndexItemInLong.pageId(positions[0]);
  }
}
