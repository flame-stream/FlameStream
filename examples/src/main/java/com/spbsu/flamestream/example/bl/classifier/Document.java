package com.spbsu.flamestream.example.bl.classifier;

import java.util.Map;

class Document {
    private final Map<String, Double> tfidf;

    public Document(Map<String, Double> tfidf) {
      this.tfidf = tfidf;
    }

  public Map<String, Double> getTfidf() {
    return tfidf;
  }
}