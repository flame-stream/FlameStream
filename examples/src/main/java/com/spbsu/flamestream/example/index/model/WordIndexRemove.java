package com.spbsu.flamestream.example.index.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class WordIndexRemove implements WordBase {
  private final String word;
  private final long start;
  private final int range;

  @JsonCreator
  public WordIndexRemove(@JsonProperty("word") String word,
          @JsonProperty("start") long start,
          @JsonProperty("range") int range) {
    this.word = word;
    this.start = start;
    this.range = range;
  }

  @Override
  @JsonProperty("word")
  public String word() {
    return word;
  }

  @JsonProperty("start")
  public long start() {
    return start;
  }

  @JsonProperty("range")
  public int range() {
    return range;
  }

  @Override
  public String toString() {
    return "REMOVE " + word + " : " + start + ", " + range;
  }
}
