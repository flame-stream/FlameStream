package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.datastream.example.invertedindex.utils.InvertedIndexStorage;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordIndex implements WordContainer {
  @JsonProperty
  private final String word;
  private final InvertedIndexStorage storage;

  public WordIndex(String word, InvertedIndexStorage storage) {
    this.word = word;
    this.storage = storage;
  }
  @Override
  public String word() {
    return word;
  }

  public InvertedIndexStorage storage() {
    return storage;
  }
}
