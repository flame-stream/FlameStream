package com.spbsu.flamestream.example.bl.classifier;

import gnu.trove.map.TObjectDoubleMap;

class Document {
  private final TObjectDoubleMap<String> tfidf;

  public Document(TObjectDoubleMap<String> tfidf) {
  this.tfidf = tfidf;
  }

  TObjectDoubleMap<String> getTfidf() {
    return tfidf;
  }
}