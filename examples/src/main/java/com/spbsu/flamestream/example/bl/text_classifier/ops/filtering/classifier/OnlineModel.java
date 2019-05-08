package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

public interface OnlineModel {
  ModelState step(DataPoint trainingPoint, ModelState prevState);
}
