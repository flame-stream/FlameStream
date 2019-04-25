package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.impl.mx.RowsVecArrayMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public interface BiClassifierOptimizer {

  Vec optimizeWeights(List<DataPoint> trainingSet, int[] isCorrect, Vec prevWeights);

  default Mx optimizeOneVsRest(List<DataPoint> trainingSet, Mx prevWeights, String[] topicList) {
    Vec[] weights = new Vec[prevWeights.rows()];
    int[][] corrects = new int[prevWeights.rows()][];
    for (int i = 0; i < prevWeights.rows(); i++) {
      final int finalI = i;
      corrects[i] = trainingSet.stream().mapToInt(s -> s.getLabel().equals(topicList[finalI]) ? 1 : 0).toArray();
    }
    IntStream.range(0, prevWeights.rows()).parallel().forEach(i -> {
      weights[i] = optimizeWeights(trainingSet, corrects[i], new SparseVec(trainingSet.get(0).getFeatures().dim()));
    });
    return new RowsVecArrayMx(weights);
  }
}
