package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

public interface OnlineModel {
  ModelState step(DataPoint trainingPoint, ModelState prevState);
}
