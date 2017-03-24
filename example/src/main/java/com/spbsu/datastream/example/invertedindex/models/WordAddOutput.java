package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by Artem on 05.02.2017.
 */
public class WordAddOutput implements WordContainer {
  @JsonProperty
  private final String word;
  @JsonProperty
  private final long[] positions;

  public WordAddOutput(String word, long[] positions) {
    this.word = word;
    this.positions = positions;
  }

  public String word() {
    return word;
  }

  public long[] positions() {
    return positions;
  }
}