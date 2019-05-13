package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Vec;

public interface TopicsPredictor {
  default void init() {
  }

  Topic[] predict(Vec vec);
}
