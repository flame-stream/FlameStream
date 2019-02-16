package com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier;

import java.util.Map;

public class Document {
  private final Map<String, Double> tfIdf;

  public Document(Map<String, Double> tfIdf) {
    this.tfIdf = tfIdf;
  }

  Map<String, Double> tfIdf() {
    return tfIdf;
  }
}