package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;

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
