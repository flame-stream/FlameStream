package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.ftrl;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.ModelState;

public class FTRLState implements ModelState {
  private final Mx weights;
  private final Mx zed;
  private final Mx norm;

  public FTRLState(Mx prevWeights) {
    weights = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    zed = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    norm = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
  }

  public Mx weights() {
    return weights;
  }

  public Mx zed() {
    return zed;
  }

  public Mx norm() {
    return norm;
  }
}
