package com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier;

public interface TopicsPredictor {
  Topic[] predict(Document document);
}
