package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by Artem on 08.02.2017.
 */
public class WordRemoveOutput implements WordContainer {
  @JsonProperty
  private final String word;
  @JsonProperty
  private final long start;
  @JsonProperty
  private final int range;

  public WordRemoveOutput(String word, long start, int range) {
    this.word = word;
    this.start = start;
    this.range = range;
  }

  public String word() {
    return word;
  }

  public long start() {
    return start;
  }

  public int range() {
    return range;
  }
}
