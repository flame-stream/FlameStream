package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

public interface TopicsPredictor {
  default void init() {
  }

  Topic[] predict(Document document);
}
