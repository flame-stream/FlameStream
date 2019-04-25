package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;

import java.util.List;

public interface Optimizer {
  Mx optimizeWeights(List<DataPoint> trainingSet, Mx prevWeights, String[] topics);
}
