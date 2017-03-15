package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.datastream.example.invertedindex.models.long_containers.LongContainer;
import com.spbsu.datastream.example.invertedindex.models.long_containers.PageLongContainer;

/**
 * Created by Artem on 05.02.2017.
 */
public class WordAddOutput implements WordContainer {
  @JsonProperty
  private final String word;
  private final PageLongContainer[] positions;

  public WordAddOutput(String word, PageLongContainer[] positions) {
    this.word = word;
    this.positions = positions;
  }

  public String word() {
    return word;
  }

  @JsonProperty
  public long[] positions() {
    return LongContainer.toLongArray(positions);
  }
}