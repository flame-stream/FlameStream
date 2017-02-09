package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import gnu.trove.list.TLongList;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordIndex implements WordContainer {
  @JsonProperty
  private final String word;
  private TLongList positions;

  public WordIndex(String word, TLongList positions) {
    this.word = word;
    this.positions = positions;
  }
  @Override
  public String word() {
    return word;
  }

  public TLongList positions() {
    return positions;
  }
}
