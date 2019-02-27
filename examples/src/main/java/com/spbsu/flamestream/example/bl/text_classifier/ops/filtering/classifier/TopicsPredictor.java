package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.impl.mx.SparseMx;

public interface TopicsPredictor {
  default void init() {
  }

  Topic[] predict(Document document);
  void updateWeights(SparseMx weights);
}
