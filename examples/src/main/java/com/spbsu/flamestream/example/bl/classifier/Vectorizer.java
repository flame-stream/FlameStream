package com.spbsu.flamestream.example.bl.classifier;

public interface Vectorizer {
  double[] vectorize(Document document);
}
