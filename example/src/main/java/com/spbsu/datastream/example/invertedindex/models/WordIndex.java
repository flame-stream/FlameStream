package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.datastream.example.invertedindex.models.long_containers.IndexLongContainer;
import com.spbsu.datastream.example.invertedindex.models.long_containers.LongContainer;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordIndex implements WordContainer {
  @JsonProperty
  private final String word;
  private final IndexLongContainer[] positions;

  public WordIndex(String word, IndexLongContainer[] positions) {
    this.word = word;
    this.positions = positions;
  }

  @Override
  public String word() {
    return word;
  }

  @JsonProperty
  public long[] longPositions() {
    return LongContainer.toLongArray(positions);
  }

  public IndexLongContainer[] positions() {
    return positions;
  }
}
