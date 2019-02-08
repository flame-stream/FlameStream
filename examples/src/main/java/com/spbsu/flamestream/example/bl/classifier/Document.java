package com.spbsu.flamestream.example.bl.classifier;

import java.util.Map;

class Document {
  private final Map<String, Double> tfIdf;

  public Document(Map<String, Double> tfIdf) {
    this.tfIdf = tfIdf;
  }

  Map<String, Double> tfIdf() {
    return tfIdf;
  }
}