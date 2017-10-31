package com.spbsu.flamestream.example.index.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexAdd implements WordBase {
  private final String word;
  private final long[] positions;

  @JsonCreator
  public WordIndexAdd(@JsonProperty("word") String word, @JsonProperty("positions") long[] positions) {
    this.word = word;
    this.positions = positions;
  }

  @Override
  @JsonProperty("word")
  public String word() {
    return word;
  }

  @JsonProperty("positions")
  public long[] positions() {
    return positions;
  }

  @Override
  public String toString() {
    return "ADD " + word + " : " + Arrays.toString(positions);
  }
}
