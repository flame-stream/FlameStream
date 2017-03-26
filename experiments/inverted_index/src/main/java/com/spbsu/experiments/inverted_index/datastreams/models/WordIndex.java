package com.spbsu.experiments.inverted_index.datastreams.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.experiments.inverted_index.common_bl.models.WordContainer;
import com.spbsu.experiments.inverted_index.common_bl.utils.InvertedIndexStorage;

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
