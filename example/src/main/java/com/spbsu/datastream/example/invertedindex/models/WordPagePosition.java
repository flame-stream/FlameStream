package com.spbsu.datastream.example.invertedindex.models;

import com.spbsu.datastream.example.invertedindex.models.long_containers.PageLongContainer;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordPagePosition implements WordContainer {
  private final String word;
  private final PageLongContainer[] positions;

  public WordPagePosition(String word, PageLongContainer[] positions) {
    this.word = word;
    this.positions = positions;
  }

  public String word() {
    return word;
  }

  public PageLongContainer[] positions() {
    return positions;
  }
}