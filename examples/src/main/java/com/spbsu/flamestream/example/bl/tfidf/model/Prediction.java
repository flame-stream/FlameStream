package com.spbsu.flamestream.example.bl.tfidf.model;

import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.Topic;

public class Prediction {
  private final TfIdfObject tfIdf;
  private final Topic[] topics;

  public Prediction(TfIdfObject tfIdf, Topic[] topics) {
    this.tfIdf = tfIdf;
    this.topics = topics;
  }

  public Topic[] topics() {
    return topics;
  }

  public TfIdfObject tfIdf() {
    return tfIdf;
  }
}
