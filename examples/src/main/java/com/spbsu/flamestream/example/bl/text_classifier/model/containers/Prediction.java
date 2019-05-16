package com.spbsu.flamestream.example.bl.text_classifier.model.containers;

import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;

public class Prediction implements ClassifierOutput {
  private final TfIdfObject tfIdf;
  private final ClassifierState state;
  private final Topic[] topics;

  public Prediction(TfIdfObject tfIdf, ClassifierState prevState, Topic[] topics) {
    this.tfIdf = tfIdf;
    this.topics = topics;
    this.state = prevState;
  }

  public ClassifierState getState() {
    return state;
  }

  public Topic[] topics() {
    return topics;
  }

  public TfIdfObject tfIdf() {
    return tfIdf;
  }
}
