package com.spbsu.datastream.example.invertedindex.models;

import gnu.trove.list.TLongList;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordPagePosition implements WordContainer {
  private final String word;
  private final TLongList positions;

  public WordPagePosition(String word, TLongList positions) {
    this.word = word;
    this.positions = positions;
  }

  public String word() {
    return word;
  }

  public TLongList positions() {
    return positions;
  }
}
