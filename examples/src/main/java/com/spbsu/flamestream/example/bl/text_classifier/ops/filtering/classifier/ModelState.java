package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;

public class ModelState {
  private final Mx weights;
  private final Mx zed;
  private final Mx norm;

  public Mx weights() {
    return weights;
  }

  public Mx zed() {
    return zed;
  }

  public Mx norm() {
    return norm;
  }

  public ModelState(Mx prevWeights) {
    weights = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    zed = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    norm = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
  }
}
