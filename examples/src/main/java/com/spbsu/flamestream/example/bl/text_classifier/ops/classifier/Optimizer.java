package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import com.expleague.commons.math.vectors.Mx;

import java.util.List;

// TODO: 13.05.19 remove me
public interface Optimizer {
  Mx optimizeWeights(List<DataPoint> trainingSet, Mx prevWeights);
}
