package com.spbsu.datastream.example.invertedindex;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordPage implements WordContainer {
  @JsonProperty
  private final int pageId;
  @JsonProperty
  private final String word;
  @JsonProperty
  private final double freq;

  public WordPage(int pageId, String word, double freq) {
    this.pageId = pageId;
    this.word = word;
    this.freq = freq;
  }

  public int pageId() {
    return pageId;
  }

  public String word() {
    return word;
  }

  public double freq() {
    return freq;
  }
}
