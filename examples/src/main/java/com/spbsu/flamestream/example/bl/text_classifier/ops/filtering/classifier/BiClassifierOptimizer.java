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

  Vec optimizeWeights(Mx trainingSet, int[] isCorrect, Vec prevWeights);

  default Mx optimizeOneVsRest(Mx trainingSet, String[] correctTopics, Mx prevWeights, String[] topicList) {
    Logger LOGGER = LoggerFactory.getLogger(BiClassifierOptimizer.class.getName());
    Vec[] weights = new Vec[prevWeights.rows()];
    IntStream.range(0, prevWeights.rows()).parallel().forEach(i -> {
      int[] corrects = Arrays.stream(correctTopics).mapToInt(s -> s.equals(topicList[i]) ? 1 : 0).toArray();
      LOGGER.info("Topic number {}, {}", i, topicList[i]);
      weights[i] = optimizeWeights(trainingSet, corrects, new SparseVec(trainingSet.columns()));
    });
    return new RowsVecArrayMx(weights);
  }
}
