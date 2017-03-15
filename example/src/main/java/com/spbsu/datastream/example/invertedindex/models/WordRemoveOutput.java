package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.datastream.example.invertedindex.models.long_containers.PageLongContainer;

/**
 * Created by Artem on 08.02.2017.
 */
public class WordRemoveOutput implements WordContainer {
  @JsonProperty
  private final String word;
  private final PageLongContainer start;
  @JsonProperty
  private final int range;

  public WordRemoveOutput(String word, PageLongContainer start, int range) {
    this.word = word;
    this.start = start;
    this.range = range;
  }

  public String word() {
    return word;
  }

  @JsonProperty
  public long start() {
    return start.value();
  }

  public int range() {
    return range;
  }
}
