package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;

public class State {
  Mx weights;
  Mx zed;
  Mx norm;

  State(Mx prevWeights) {
    weights = prevWeights;
    zed = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    norm = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
  }
}
